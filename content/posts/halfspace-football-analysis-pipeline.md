+++
title = 'Building a Real-Time Football Analysis Pipeline: What I Learned About Distributed Systems'
date = 2026-06-02
draft = false
+++

*A technical account of building halfspace — a real-time football match analysis system — and what went wrong, what the evals revealed, and what I'd do differently.*

---

## The idea

StatsBomb publishes detailed football event data — every pass, shot, pressure, carry — with rich attributes like xG, defensive positioning, and possession context. Clubs and media companies use their live platform to watch these metrics update in real time on matchday. Human analysts interpret what they see and produce tactical commentary.

I wanted to automate that interpretation layer. Not replace the human collection (that requires trained annotators tagging spatial data in real time), but sit downstream of it and generate the analysis automatically.

The result: a pipeline that ingests events through Kafka, computes metrics in a stateful stream processor, and triggers Claude to write half-time reports and live observations as the match happens.

---

## The architecture

Six services, two message queues, one workflow engine:

```
Replayer → Kafka → Processor → Postgres
                             → Kafka (triggers) → Temporal → Claude → Reports
API ← Kafka + Postgres + Reports
```

Everything communicates through Kafka. The processor never calls the agent directly. The agent never calls the processor. This decoupling turned out to be the right call — it meant I could restart any service independently without losing data.

---

## Exactly-once processing: the real problem

My first instinct was to write a simple Kafka consumer. Consume an event, update the metrics, move on. But Kafka guarantees at-least-once delivery. A crash between processing an event and committing the offset means that event is re-delivered. Without a defence, metrics get double-counted.

I ended up with four layers:

1. **In-memory dedup** — every StatsBomb event has a UUID. The processor tracks seen IDs per match and skips duplicates.
2. **Idempotent Postgres upsert** — the metrics table has a unique constraint on `(match_id, team_id, window_name, match_clock)`. `INSERT ... ON CONFLICT DO UPDATE` means re-processing the same batch produces identical rows.
3. **Offset commit after write** — offsets are committed only after a successful Postgres write. If the write fails, the offset isn't committed and the events are re-delivered. Combined with the idempotent upsert, this makes the system exactly-once end-to-end.
4. **Temporal workflow deduplication** — each workflow's ID is the trigger ID. Temporal rejects a second workflow with the same ID if one is running. A double-delivered trigger produces one report.

The insight: exactly-once isn't a single mechanism. It's a stack. Remove any layer and a different failure mode leaks through.

---

## Why Temporal — and what it costs

The LLM call is the most expensive thing in the pipeline — not in CPU, in consequence. If the agent crashes after calling Claude but before saving the report, that tactical moment is gone. The match moved on.

Temporal persists the full workflow execution history to Postgres. If a worker dies mid-workflow, a restarted worker replays history, skips already-completed activities, and continues from where it left off. The LLM is not called twice. The report is not lost.

The alternative — a Kafka trigger consumer that calls Claude directly and writes the report — is simpler and has fewer moving parts. But if the process dies between the Claude call and the file write, the report is lost. You'd need to build your own idempotency key check, retry logic, and dead-letter handling. That's most of what Temporal is, minus the UI and the proven production hardening.

**What Temporal costs you:**

*One more service to operate.* Temporal server plus its own Postgres schema — two extra containers in docker-compose. More moving parts to start, healthcheck, and reason about. Cold start takes 20-30 seconds; the agent can't register until Temporal is healthy, so the dependency chain matters.

*Replay constraints on workflow code.* Workflow functions must be deterministic. You cannot use `time.Now()`, random numbers, or non-deterministic map iteration directly in workflow code — the workflow is replayed from history on restart and must produce the same decisions each time. All I/O lives in activities; the workflow is pure control flow. This is a real mental model shift that catches you the first time you reach for `time.Now()` inside a workflow.

*Overkill at low volume — but almost free at the scale we care about.* Temporal Cloud charges ~$25 per million workflow actions. One match uses roughly 8 actions (4 activities × 2 workflows). At 3,000 matches/month that's $0.60 — negligible. The self-hosted version we run is free.

**The honest tradeoff in one sentence:** Temporal trades operational complexity at setup time for operational simplicity at failure time — the failures that matter most (lost LLM calls, duplicate reports) are handled for you without writing a line of retry logic.

---

## Keying Kafka by match_id

Every Kafka record is keyed by `match_id`. This means all events for one match land on the same partition, and one processor instance owns that partition. The processor sees events in strict arrival order with no coordination — no distributed locking, no cross-instance synchronisation.

The payoff: the in-memory match state is always correct because only one consumer ever touches it. The downside: if that instance crashes, the partition rebalances to another instance, which rebuilds state by replaying from the last committed offset. This is acceptable — replay is cheap, Postgres is durable, and the idempotent upsert handles the re-delivered tail.

---

## Running three matches concurrently

The first version had one global WebSocket hub broadcasting everything to everyone. When I added multi-match support, I realised a client watching Belgium vs Japan shouldn't receive events from France vs Croatia.

The fix was `HubManager` — one isolated hub per match ID. `pollKafka` extracts `match_id` from each envelope and routes to the right hub. `pollMetrics` groups Postgres snapshots by match and broadcasts independently. Each client connects to `/matches/{id}/ws` and only receives events for that match.

Under 3-match concurrent load with resource limits enforced (processor: 0.5 CPU / 128MB, agent: 0.5 CPU / 256MB), the services ran at 2–7% of their limits. The bottleneck at scale is LLM throughput, not compute.

---

## The eval framework: where things got interesting

I built four eval pillars and wired scores into Langfuse. This was the most educational part of the project.

**Structural** (free, instant): does the report have the right shape? Five required sections for half-time. Two or three sentences for live observations.

**Metric grounding** (free, code-based): parse numbers from the report text, diff against the actual metrics in Postgres. A number "matches" if it's within tolerance of any metric value in the snapshot.

**LLM-as-judge** (costs ~$0.001/report): a second Claude call reads the report and the actual metrics, then scores ACCURACY, ACTIONABILITY, and BREVITY from 1 to 10.

**Q&A trajectory** (free): count the tool-call rounds in the Q&A agent loop. One round is perfect (1.0), six or more is poor (0.2).

The scores were useful. The *comments* were the bug reports.

The LLM-as-judge comment on one report:

> "Misidentifies Team 779 as France when metrics show Argentina dominated (70% tilt, 60.7% possession, 535 passes)"

That identified a two-source-of-truth bug. I had a guard that validated the perspective team against the match teams before building the prompt, but the LLM's system prompt identity — "You are France's analyst" — was set separately from a raw config value that bypassed the guard. The report content was neutral; the LLM's identity was still France. It hallucinated France into an Argentina match.

Without the eval, that report looked plausible on first read. With the eval, it was a specific bug pointing at a specific code path.

---

## What the fixes taught me about LLM enforcement

Three bugs, three different fix types, three different reliability levels:

**Prompt instruction** (grounding): "Only cite numbers that appear explicitly in the data above." Reduced fabrication. Didn't eliminate it — the model treats instructions as suggestions when the alternative feels more analytical.

**Hard constraint** (token limit): Dropped MaxTokens from 120 to 90 to enforce shorter live observations. The model adapted by writing shorter sentences to fit four of them in. Token limits constrain characters, not sentences.

**Code enforcement** (sentence truncation): After the LLM responds, count the full stops in code. If there are more than three, truncate at the third. The structural score went from 0.68 average to 0.97 in the next run.

Rule of thumb: if the behaviour you want is measurable in code, enforce it in code. Use prompt instructions only for things that can't be measured deterministically.

---

## What I'd do differently

**Provide rolling window data in the prompt from the start.** The half-time prompt showed only cumulative totals. The LLM filled the gap by inventing time-series progressions. I had the per-window data in the database and was already fetching it — I just hadn't included it. Grounding went up when I added the instruction not to fabricate; it would have gone up more if I'd provided the actual trend data.

**Add evals on day one.** I added them at the end and they immediately found bugs that had been there the whole time. An eval that runs on every report means every prompt change shows up in the score history. Without that, you're guessing whether a prompt change helped.

**Think harder about reports storage.** Writing reports to a bind-mounted filesystem is simple and fast. But it means horizontal scaling of the API requires a shared volume. A message-passing approach — where the agent broadcasts the report content directly to the hub rather than writing to disk and watching for file changes — would be cleaner and more scalable.

---

## The numbers

Three matches, one run:
- 32 LLM calls (6 half-time reports, 26 live observations)
- $0.033 total — ~$0.011 per match
- Structural score: 0.97 average
- Quality score: 0.76 average
- Peak memory: 8.8MB (processor), 17.9MB (agent)
- CPU at peak: 2.24% of a 0.5-core limit

The system is comfortably within its resource limits at 3 concurrent matches. The cost is dominated by LLM calls, not infrastructure.

---

## What this is and isn't

This is an event data pipeline. StatsBomb's moat is the human collection layer — trained annotators tagging spatial attributes in real time (goalkeeper position, defender positions at the moment of a shot) that power their xG model and 3D freeze frames. That can't be automated with what's in the open dataset.

What can be automated is the analysis and narration layer downstream of that data. That's what halfspace does — read the numbers, write the interpretation, flag the tactical signals.

The gap between "StatsBomb data exists" and "real-time AI tactical analysis" turned out to be mostly a distributed systems problem. Exactly-once semantics, durable workflow execution, partitioned state, backpressure — these were the hard parts. The LLM integration was straightforward once the data was reliable.
