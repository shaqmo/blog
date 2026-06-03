---
title: 'From Live Match Events to AI Tactical Reports: Building a Real-Time Pipeline with Kafka and Temporal'
date: 2026-06-02
draft: false
---

*A design journal of the decisions made, the things that broke, and what the final system actually looks like — building a real-time football analysis pipeline as an experiment in distributed systems.*

**<a href="https://shaqmo.github.io/blog/halfspace-demo/" target="_blank" rel="noopener noreferrer">▶ Live demo — replay three 2018 World Cup matches</a>**

*The demo is a pre-recorded replay of the live system — real events, metrics, and AI-generated reports captured from an actual run, replayed in your browser. The underlying pipeline runs in real time against a live Kafka stream; the demo lets you see the output without standing up the infrastructure.*

---

## The experiment

StatsBomb publishes detailed football event data — every pass, shot, pressure, carry — with rich attributes like xG, defensive positioning, and possession context. Clubs and media companies employ trained analysts to interpret these numbers live on matchday and generate tactical commentary.

The question I wanted to answer: can you automate the interpretation layer using a message-driven pipeline and an LLM? Not the data collection — that requires human annotators tagging spatial attributes in real time, which is StatsBomb's actual moat. But the analysis that sits downstream of that data.

Turns out the interesting part wasn't the AI. It was the distributed systems problems that had to be solved before the AI could do anything reliable. This is a record of those decisions.

---

## The stack

Before getting into decisions, here is what the system is made of and why each piece is there.

```text
┌──────────┐   match.events    ┌───────────┐   window_metrics   ┌──────────┐
│ Replayer │ ────────────────► │ Processor │ ────────────────►  │ Postgres │
└──────────┘                   └───────────┘                    └──────────┘
                                     │
                               match.triggers
                                     │
                                     ▼
                                ┌─────────┐      ┌──────────┐      ┌────────┐      ┌─────────┐
                                │  Agent  │ ───► │ Temporal │ ───► │ Claude │ ───► │ Reports │
                                └─────────┘      └──────────┘      └────────┘      └─────────┘

┌─────────┐   WebSocket   ┌─────┐
│ Browser │ ◄──────────── │ API │ ◄── Kafka · Postgres · Reports
└─────────┘               └─────┘
```

**Kafka** is the backbone. Every service communicates through topics — no direct service-to-service calls. The replayer publishes raw match events. The processor consumes them and publishes trigger events. The API consumes events to push to the browser. Kafka was chosen over a message queue like RabbitMQ because it retains messages on disk — consumers can replay history, restart without losing data, and join mid-stream. And that replay property turns out to be pretty critical for exactly-once semantics and crash recovery.

**The Replayer** reads StatsBomb match JSON files and emits events onto Kafka at configurable speed — 10× real time by default, compressing a 90-minute match into 9 minutes. It simulates a live data feed using the same open data StatsBomb provides to clubs and media companies.

**The Processor** is a stateful Kafka consumer. It consumes raw events and computes tactical metrics — possession percentage, expected goals (xG), PPDA (passes allowed per defensive action, a measure of pressing intensity), field tilt, pass count, and pass accuracy — across four rolling time windows: cumulative, last 5 minutes, last 10 minutes, and last 15 minutes. Every 5 minutes of match time it writes a snapshot to Postgres. When a period boundary (`Half End`) is detected, it publishes a trigger event onto a second Kafka topic. No framework — plain Go consuming a Kafka topic, updating in-memory state, writing to Postgres.

**Temporal** is a durable workflow engine. When the agent receives a trigger from Kafka, it starts a Temporal workflow rather than calling Claude directly. A workflow is a sequence of activities — fetch metrics from Postgres, build a prompt, call Claude, save the report — where every completed step is checkpointed. If the worker crashes mid-workflow, it resumes from the last checkpoint without re-running completed steps. This is the answer to "what happens if the process dies while Claude is generating a 400-word report?"

**Claude** (via the Anthropic API) is the LLM. It receives a structured prompt containing the current match metrics and returns a tactical report — either a half-time analysis or a short live observation. It runs inside a Temporal activity, so it is retried automatically on transient failures and never called twice for the same report.

**Postgres** serves two roles: the metrics store (written by the processor, read by the agent and API) and the Temporal history store (written and read by Temporal itself to checkpoint workflow state). A single Postgres instance, two schemas.

**The API** is a Go HTTP server that bridges Kafka, Postgres, and the browser over WebSocket. It consumes the `match.events` topic independently to populate a replay buffer, polls Postgres every 5 seconds for the latest metric snapshots, and watches the reports directory for new files. All three streams are multiplexed into a single WebSocket connection per client. A plain HTML/JS frontend connects to it — no framework, no build step, one static file served by the same binary.

---

## Decision 1: where does match state live?

The pipeline needs to compute metrics — possession, xG, PPDA, field tilt — from a stream of events. The events arrive in order per match, but when running three matches simultaneously, events from different matches interleave on the same topic.

**First instinct:** one processor consuming all events, tracking all matches in a single in-memory map keyed by `match_id`.

**The problem:** as soon as you think about horizontal scaling — running two processor instances to handle load — that shared map breaks. Two instances would each see a partial event stream for any given match, producing incorrect metrics. You'd need distributed locking or a shared external store, both of which introduce coordination overhead.

**The decision:** key every Kafka record by `match_id`. This routes all events for one match to the same partition, and Kafka's partition model guarantees that one consumer instance owns each partition. No two processor instances ever touch the same match concurrently. In-memory state is always correct because there's only one writer.

The tradeoff: if that instance crashes, the partition rebalances to another instance, which must rebuild state by replaying from the last committed offset. This is acceptable — replay is cheap, the events are already in Kafka, and the metric computation is deterministic so replaying produces the same result.

**Final state:** all Kafka records keyed by `match_id`. Processor holds match state in `map[int]*MatchState`, one entry per partition owned. No locking needed between matches.

---

## Decision 2: making delivery exactly-once

Kafka guarantees at-least-once delivery. A crash between processing an event and committing the offset means the event is re-delivered. Without a defence, metrics get double-counted — possession events counted twice, xG inflated, shot counts wrong.

I worked through this in layers, each one plugging a different failure mode:

**Layer 1 — In-memory dedup.** Every StatsBomb event has a UUID. The processor tracks seen IDs per match in a `map[string]bool`. Duplicate delivers are caught and skipped immediately. Problem: this map is lost on restart.

**Layer 2 — Idempotent Postgres upsert.** The metrics table has a unique constraint on `(match_id, team_id, window_name, match_clock)`. `INSERT ... ON CONFLICT DO UPDATE` means re-processing the same batch — after a restart, replaying the event log — produces identical rows. The database never double-counts even if the processor sees the same events twice.

**Layer 3 — Offset commit after write.** Offsets are committed only after a successful Postgres write. If the write fails, the offset isn't committed and the events are re-delivered on the next poll. Combined with the idempotent upsert, this makes the stream processing exactly-once end-to-end even across crashes.

**Layer 4 — Temporal workflow deduplication.** The metric processor emits a trigger when a period boundary is reached. That trigger starts a Temporal workflow. The workflow ID is the trigger ID. If the trigger is delivered twice to the Kafka consumer, Temporal rejects the second `ExecuteWorkflow` call with `WorkflowExecutionAlreadyStarted` — one report per trigger, no matter how many times the trigger arrives.

**What I learned:** exactly-once isn't a single mechanism. It's a stack. Remove any layer and a different failure mode leaks through — the in-memory dedup handles the common case cheaply, the idempotent upsert handles restarts, the offset-after-write handles write failures, the workflow dedup handles duplicate triggers. Each layer has a specific responsibility.

---

## Decision 3: how to survive the LLM call failing

The LLM call is the most consequential step in the pipeline — not in CPU cost, but in consequence. If the agent crashes after calling Claude but before saving the report, that tactical moment is gone. The match moved on.

**The simple alternative** is a Kafka trigger consumer that calls Claude directly, writes the report to disk, and commits the offset. This is less moving parts. But if the process dies between the Claude call and the file write, the report is lost. You'd need to build your own idempotency key check, your own retry with exponential backoff, your own dead-letter handling. That's basically most of what Temporal is, minus the UI and the production hardening.

**The decision:** Temporal. It persists the full workflow execution history to Postgres. If a worker dies mid-workflow, a restarted worker replays history, skips already-completed activities, and continues from where it left off. I saw this directly: killing the agent process mid-`CallLLMActivity` and restarting — Temporal resumed from `SaveReportActivity`. The LLM was not called again. The report was not lost.

```
FetchMetrics  → returns cached result (already completed)
BuildPrompt   → returns cached result (already completed)
CallLLM       → returns cached result (already completed)
SaveReport    → executes fresh ← only this re-runs
```

**What Temporal costs:** one more service (Temporal server plus its own Postgres schema — two extra containers). Replay constraints on workflow code — you cannot use `time.Now()`, random numbers, or non-deterministic map iteration directly in a workflow function because the workflow is replayed from history on restart and must produce the same decisions each time. All I/O lives in activities, the workflow is pure control flow. This catches you the first time you reach for `time.Now()` inside a workflow.

**Retry policies per activity:** different activities have different failure modes and tolerances.

```
FetchMetricsActivity  → 5 attempts, 10s initial, 2× backoff, 60s max
                        Handles processor lag after Half End trigger.

BuildPromptActivity   → 2 attempts, 1s initial
                        Pure function, nearly impossible to fail.

CallLLMActivity       → 5 attempts, 5s initial, 2× backoff, 60s max
                        Transient network errors, rate limits, Anthropic timeouts.

SaveReportActivity    → 3 attempts, 2s initial
                        File write — short backoff is sufficient.
```

**Final state:** Temporal handles LLM call durability. Every report that starts generating completes, even across worker restarts. The simpler alternative would require rewriting the retry and idempotency logic that Temporal provides.

---

## Decision 4: WorkflowID dedup is not enough on its own

I assumed `WorkflowID = TriggerID` was a complete solution to duplicate reports. It isn't.

Temporal's WorkflowID dedup only prevents concurrent duplicates — two `ExecuteWorkflow` calls while the first run is still in flight. Once the first run completes, Temporal accepts a second `ExecuteWorkflow` with the same ID and starts a fresh run. Workflow history would be unbounded if Temporal tracked IDs forever.

**The bug:** StatsBomb emits two `Half End` events per period, one per team. Both passed through the processor and both triggered a workflow with the same ID. The first trigger arrived while the workflow was running (concurrent — Temporal correctly dedupped it). But sometimes the second trigger arrived after the first workflow completed — sequential. Temporal accepted it. A second report was generated.

**Three options considered:**

| Option | Trade-off |
|---|---|
| Seen-set in the agent Kafka consumer | Stateful, lost on restart |
| Catch `WorkflowExecutionAlreadyStarted` error | Still produces a second Kafka message; wastes work |
| Fix the source — emit on `Half End` only, mark period as triggered | No duplicates ever produced |

**The decision:** fix the source. The processor now tracks `triggeredPeriods map[int]bool` on `MatchState`. After emitting the first trigger for a period, it marks that period as done and skips all subsequent boundary events for it. Temporal dedup remains as a backstop for genuine double-delivery, not as the primary guard.

The lesson: Temporal (or any dedup mechanism) handles concurrent duplicates well. Sequential duplicates — where the same logical event arrives again after the first processing completes — these require source-level suppression.

---

## Decision 5: routing events to the right client

The first version had one global WebSocket hub. Every event from every match was broadcast to every connected client. When I added a second match, a client watching Belgium vs Japan received France vs Croatia events. Filtering on the frontend was possible but moved the routing logic to every client.

**The decision:** `HubManager` — one isolated hub per match ID, created lazily when the first event for that match arrives. Three separate goroutines route to the right hub:
- `pollKafka` extracts `match_id` from each event envelope and routes to the correct hub
- `pollMetrics` groups Postgres snapshots by match and broadcasts independently per match
- `watchReports` reads `match_id` from the report filename and routes to the right hub

Clients connect to `/matches/{id}/ws` and only receive events for that match.

**Landing page:** two sources of truth for "which matches are active" — the HubManager (populated as Kafka events arrive) and `window_metrics` in Postgres (populated after the first snapshot). Neither alone is correct at all points in the lifecycle — events may arrive before the first metric snapshot, or a page refresh may happen after all events are done but metrics remain. The landing page merges both sets.

**Final state:** per-match isolation at the hub level. Routing is server-side. The client receives only one match's events, regardless of how many matches are running concurrently.

---

## Decision 6: how to make metric snapshots work on an event-driven stream

The first design triggered metric snapshots on a time-based ticker — every 5 minutes of real time. This was wrong for a replay scenario where match time advances much faster than wall time.

**The decision:** snapshot on event-driven clock boundaries. The processor computes match time from the event's `minute` and `second` fields. When an event crosses a 5-minute match-clock boundary, a snapshot is written. No wall-clock ticker.

**The consequence discovered in production:** StatsBomb events are match actions — passes, shots, pressures. They're not clock ticks. A quiet spell in the match (the ball goes out, substitutions, VAR checks) means no events arrive. If no event crosses the 5-minute boundary, the snapshot doesn't fire until the next event arrives, which might be at minute 7. So the snapshots are event-driven, not evenly spaced.

For a production system with real-time delivery, a separate time-based ticker would fire alongside the event consumer to guarantee snapshots at fixed intervals regardless of match activity. For this experiment, event-driven snapshots are sufficient — the match data is dense enough.

---

## Decision 7: LLM enforcement hierarchy, discovered through evals

The eval framework was added at the end, but it immediately found bugs that had been present from the start. The evals produce scores. The scores were useful, the *comments* were the bug reports.

**Bug 1 — Phantom team in system prompt.** The LLM-as-judge comment on one report:

> "Misidentifies Team 779 as France when metrics show Argentina dominated (70% tilt, 60.7% possession, 535 passes)"

This pointed at a two-source-of-truth bug. `FetchMetricsActivity` correctly validated `perspectiveTeam` against the actual teams in the match and cleared it when France wasn't playing. But `CallLLMActivity` and `CallLiveObsLLMActivity` read `a.perspectiveTeam` directly — the raw, unvalidated config value — when building the system prompt. The report body was neutral, the LLM's identity was still "You are France's analyst." It hallucinated France into an Argentina match.

**Fix:** both LLM call activities now accept `perspectiveTeam string` as an explicit parameter. The workflow passes `metrics.MatchCtx.PerspectiveTeam` — the value that has already passed the guard. The raw `a.perspectiveTeam` is never used in LLM calls. One source of truth.

**Bug 2 — Live observations consistently too long.** Structural score averaged 0.68 because ~60% of live observations were four sentences against a three-sentence target. Three fix attempts, three different reliability levels:

1. **Prompt instruction** — "Write EXACTLY 2 sentences." The model treated this as a suggestion, especially when the content felt like it warranted more. Score improved marginally.

2. **Token limit** — Dropped `MaxTokens` from 120 to 90. Effective for character count. The model adapted by writing shorter sentences, fitting four of them into the lower budget. Token limits constrain characters, not sentences.

3. **Code enforcement** — `truncateToSentences(text, 3)` applied after the LLM responds. Count terminal punctuation in code; if there are more than three, truncate at the third. 100% reliable regardless of model behaviour.

The structural score went from 0.68 to 0.97 between runs 1 and 3. The code enforcement was the decisive fix.

**The pattern across all three runs:**

| Pillar | Run 1 (baseline) | Run 2 (phantom+brevity) | Run 3 (all fixes) |
|---|---|---|---|
| `structural` | 0.68 | 0.71 | **0.97** |
| `grounding` | 0.63 | 0.64 | **0.71** |
| `quality` | 0.61 | 0.63 | **0.76** |

**Rule:** if the behaviour you want is measurable in code, enforce it in code after the LLM responds. Use prompt instructions only for things that can't be measured deterministically — tone, reasoning quality, relevance.

---

## What the final system looks like

Six services, communicating through Kafka and Postgres, with one workflow engine:

```
Replayer ──► Kafka (match.events) ──► Processor ──► Postgres
                                                  └──► Kafka (match.triggers) ──► Temporal ──► Claude ──► Reports
API ◄── Kafka + Postgres + Reports (disk)
```

**Kafka:** all records keyed by `match_id`. All events for one match go to one partition. One processor instance owns each partition — no distributed locking, no cross-instance coordination.

**Processor:** stateful stream processor. Exactly-once via 4-layer stack (in-memory dedup, idempotent upsert, offset-after-write, Temporal workflow dedup). Emits metric snapshots on event-driven clock boundaries. Emits period-boundary triggers on the first `Half End` per period.

**Temporal:** durable workflow execution. Each workflow is `FetchMetrics → BuildPrompt → CallLLM → SaveReport`. Worker crashes are recovered without re-calling the LLM. Retry policies tuned per activity. WorkflowID = TriggerID as a concurrent-dedup backstop.

**Agent:** separate method per LLM call type (`CallLLMActivity` for half-time, `CallLiveObsLLMActivity` for live observations). Temporal activity registration is by method name — separate methods means no signature changes, safe to deploy alongside existing workflow history. Eval activities are non-fatal (`MaximumAttempts: 1`) and run after `SaveReport` — eval failure never blocks a report.

**API:** HubManager with one hub per match. Three goroutines routing to the right hub from Kafka, Postgres, and disk. WebSocket ring buffer (5,000 slots) for replay on page load. Consumer group with unique ID per process start so historical events always replay from the beginning.

**Eval framework:** four pillars. Structural and grounding evals are free and deterministic. LLM-as-judge costs ~$0.001/report but generates comments, not just scores. Q&A efficiency scored by round count. All scores attached to the Langfuse trace ID of the generating call — report and scores visible in one trace view.

---

## What I would do differently

**Provide rolling window data in the prompt from the start.** The half-time prompt showed only cumulative totals. The LLM filled the gap by inventing time-series progressions — "possession climbed to 71% in the final 15 minutes" when the data only showed a single cumulative figure. The per-window data was already in Postgres, I just didn't include it. That one oversight inflated the fabrication rate for the entire experiment.

**Add evals on day one.** The phantom team bug had been there since the live observation workflow was written. It was invisible in code review and in casual output inspection. The LLM-as-judge comment found it in the first eval run. An eval running on every report means every prompt or code change shows its effect on the score history. Without it, you're guessing.

**Fix the reports storage model.** Writing reports to a bind-mounted filesystem is simple. It also means the API service cannot scale horizontally without a shared volume, and the report file watcher is a polling loop that adds 2-second latency. A cleaner design: the agent broadcasts report content directly onto a Kafka topic, and the API subscribes to it like everything else. Reports become a first-class event in the stream, not a filesystem side-effect.

---

## The numbers

Three matches, one run, at 20× replay speed:

| Metric | Value |
|---|---|
| LLM calls | 32 (6 half-time reports, 26 live observations) |
| Total cost | $0.033 (~$0.011/match) |
| Half-time report latency | 10s average, 12.4s max |
| Live observation latency | 1.9s average |
| Structural score | 0.97 average |
| Quality score | 0.76 average |
| Processor peak memory | 8.8 MB / 128 MB limit |
| Agent peak memory | 17.9 MB / 256 MB limit |
| Processor peak CPU | 2.24% of 0.5-core limit |

The pipeline runs at 2–7% of its resource limits under 3-match concurrent load. The bottleneck at scale is LLM throughput — specifically Anthropic's rate limit — not compute.

Turns out the gap between "StatsBomb data exists" and "real-time AI tactical analysis" was mostly a distributed systems problem: exactly-once semantics, durable workflow execution, partitioned state, per-match routing. The LLM integration was the straightforward part once the data layer was reliable.
