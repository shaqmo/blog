---
title: 'How RAG, LangGraph, and Structured Validation Can Bridge the Gap Between Proprietary Data Schemas and a Universal Football Language'
date: 2026-05-12
draft: false
---

*How RAG, LangGraph, and structured validation can bridge the gap between proprietary data schemas and a universal football language.*

---

If you've ever tried to compare player statistics across competitions — Premier League vs La Liga, Champions League vs World Cup — you've probably hit a wall that has nothing to do with the quality of the data.

The problem isn't missing data. It's that the same actions are defined differently by different providers.

StatsBomb's "pressure" event is not the same as Wyscout's. One company's "cross" captures a different set of actions than another's. When you try to combine datasets from multiple providers, the numbers stop meaning the same thing. You're not comparing apples to apples anymore.

This is a well-known problem in football analytics. FIFA's data engineer Sirus Saberi laid it out plainly at the StatsBomb Conference in 2023:

> "These data providers potentially might have used different sets of definitions to generate the data... that would render your data inconsistent because you wouldn't be able to compare different tournaments with each other."

FIFA's solution — developed over 2.5 years by Chris Loxston's team alongside Arsène Wenger — was to publish the FIFA Football Language: a canonical vocabulary with precise, unambiguous definitions for every meaningful football action. 217 terms covering in-possession, out-of-possession, in-contest, and goalkeeping. A lingua franca for football data.

The idea is simple: if every provider translates their data into FIFA's vocabulary, the data becomes comparable regardless of source.

I built a system to do exactly that — automatically.

---

## The Project: Lingua

Lingua takes StatsBomb open event data and translates it into FIFA Football Language tags at the possession level.

A "possession" is a spell of play while one team has the ball — the 2022 World Cup Final between Argentina and France had 255 of them. For each one, the system reads the StatsBomb events, retrieves the most relevant FIFA vocabulary terms, asks Claude to apply them, validates the output against a strict schema, and returns a structured record.

For the Di María goal in the final:

```
[00:35:14] Di María — Ball Receipt at (105, 35)
[00:35:14] Di María — Carry → (114, 37)
[00:35:16] Di María — Shot → Goal, xG=0.30
[00:35:17] Lloris   — Goal Keeper
```

The system produces:

```json
{
  "phase": "final_third",
  "distribution_type": "attempt_at_goal",
  "possession_outcome": "goal",
  "receiving_action": "received_from_pass",
  "line_break": true,
  "attempt_outcome": "goal",
  "attempt_pressure": "none"
}
```

Every value is a real FIFA Football Language term. Every field is enforced by a Pydantic schema — the model cannot hallucinate a term that doesn't exist.

---

## How It Works

**The knowledge base.** I compiled all 217 FIFA Football Language terms from the FIFA Training Centre into a structured taxonomy. Each term has its field, value, category, definition, and an embedding-optimised text description. This taxonomy is the ground truth the system reasons against.

**RAG — not stuffing.** Rather than sending all 217 terms to the LLM on every call, I embed them into a Chroma vector store using a local sentence-transformers model (no OpenAI dependency). For each possession, the system retrieves the 12 most semantically relevant terms. This keeps prompts lean and focused — the model sees what's relevant, not everything.

**The LangGraph agent.** The agent is a three-node state machine: retrieve terms → generate tags → validate output. If Claude returns a value that isn't in the FIFA vocabulary, Pydantic rejects it immediately. The validation error goes back to Claude with a retry prompt listing the valid values. Max two retries.

**Structured output enforcement.** The Pydantic schema uses `Literal` types — each field is constrained to exact FIFA vocabulary values. `phase` can only be one of nine defined values. `distribution_type` can only be one of five. If the model returns anything else, the schema rejects it. This is the difference between a system that works and one that produces plausible-sounding nonsense.

**Observability with LangSmith.** Every agent run is traced — which terms were retrieved, what prompt was sent, what the model returned, whether validation passed, how many retries were needed. When something goes wrong, you can see exactly why.

---

## The Eval: Where It Works and Where It Doesn't

Building the system was one thing. Measuring it honestly was more interesting.

I hand-tagged 20 possessions from the WC Final using FIFA's published definitions — the same methodology FIFA uses to train their own analysts. Then I ran the system over the same possessions and compared field by field.

**Schema validity: 100%.** Every possession produced valid FIFA vocabulary output on the first try. Zero retries needed across all 20 possessions. The validation loop worked, and once the prompt included explicit valid values, Claude stopped hallucinating.

**Overall agreement: 31%.** Lower than I'd like, but the breakdown is what matters.

Where it worked:

- `line_break` — 75% agreement. Claude reliably detects whether a possession broke through a defensive line.
- `possession_outcome` — 50%. Reasonable on how possessions end.
- `phase` — 45%. Decent on tactical phase classification.

Where it failed:

- `receiving_action` — 10%. The system is consistently wrong about how possession started.
- `receiving_location` — 6%. Near-zero agreement.
- `receiving_pressure` — 7%. Same.
- `pass_type` — 0%. Complete miss.

The pattern is clear and expected: the system works on outcome-based fields that are directly inferable from StatsBomb events. It fails on context fields that require knowing the opposition's defensive shape — information StatsBomb's event data doesn't contain.

This isn't a model failure. It's a data failure. StatsBomb tells you what Di María did. It doesn't tell you where the French defensive units were when he received the ball. Without that information, no amount of prompt engineering will reliably classify `receiving_location`.

This is an important distinction for anyone building LLM-powered analytics tools: when your system is wrong, the first question isn't "is the model bad?" It's "does the input data contain the information needed to answer this question?"

In this case, some fields require tracking data — continuous 25fps positional data for all 22 players — which StatsBomb's open dataset doesn't include. I documented this explicitly as a tiered translatability model: some fields are translatable from event data alone, some require 360 freeze-frame data, and some require full tracking data. Knowing that boundary is as valuable as the system itself.

---

## What I Learned

**The vocabulary is the hard part.** Building the RAG pipeline and the LangGraph agent took time, but the genuinely difficult work was compiling and structuring the FIFA taxonomy. 217 terms with precise definitions, scraped from the FIFA Training Centre and converted into a structured format the embedding model could reason about. Without a high-quality knowledge base, the retrieval step is useless regardless of how good the rest of the pipeline is.

**Validation failures are information.** The retry loop — where Pydantic rejects invalid output and sends the error back to Claude — turned out to be a diagnostic tool as much as a reliability mechanism. Watching which fields fail validation most often told me where the prompt needed to be more explicit about valid values. The system taught me where it was confused.

**Eval methodology matters as much as eval numbers.** A 31% overall agreement score could sound like failure. But knowing that `line_break` is 75% and `pass_type` is 0%, and understanding why each number is what it is, is more valuable than a single aggregate metric. The per-field breakdown is the actual insight — it tells you which parts of the translation problem are tractable with this data and which aren't.

**RAG is a precision tool, not a magic one.** Retrieving the 12 most relevant FIFA terms per possession works well for outcome-based classification because the semantic similarity between "shot that went in" and `attempt_at_goal: goal` is high. It works poorly for context-based fields because the possession text doesn't contain the information those fields require. Better retrieval won't fix a missing-data problem.

---

## What's Next

The immediate extension is adding StatsBomb 360 freeze-frame data — positional snapshots of all visible players at the moment of each event. This would unlock the context fields that are currently near-zero: receiving location, receiving pressure, defensive unit configuration. It's a well-defined engineering problem and the data is available.

The longer-term question is whether a system like this could become a practical tool for federations or clubs wanting to translate historical StatsBomb data into FIFA's vocabulary for cross-competition analysis. The architecture is provider-agnostic by design — a new ingestion layer for Wyscout or Opta would slot in without changing the agent, schema, or knowledge base.

The FIFA Football Language is an open educational framework, free to use. The translation problem is real, the vocabulary is published, and the data is available. The main remaining gap is data completeness — not model capability.

---

If you're working on football data standardisation, building RAG pipelines for sports analytics, or just want to discuss the approach — I'd love to connect. Feel free to reach out via LinkedIn or drop a comment below.
