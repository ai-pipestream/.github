# Pipestream AI Public Research

We publish our research so it can be reviewed, reproduced, and improved on. This
page collects the work in progress. Everything referenced here is either open
source under Apache 2.0 / MIT or filed as an open pull request against the
relevant upstream project.

---

## Collaborative distributed HNSW search

**Status:** active · **Upstream:** [Apache Lucene PR #16357](https://github.com/apache/lucene/pull/16357) · **Reference engine:** [distributed-search](https://github.com/ai-pipestream/distributed-search)

HNSW graphs give excellent approximate nearest-neighbour recall on a single
node. Distributing that search across shards is where the cost reappears: the
common approach queries every shard to its full search depth and merges the
results afterward, so each shard does as much work as if it held the whole index.

We are investigating a **collaborative** approach. Instead of each shard
searching in isolation, shards share a running *floor* — the score of the
weakest result currently good enough to make the global top-*k*. A shard that
can prove it cannot beat the floor stops early. The goal is global-quality recall
at a fraction of the aggregate work.

The first upstream piece of this is a **shared-floor kNN collector**, proposed to
Lucene's sandbox module:

- [Add shared-floor kNN collection to the sandbox module](https://github.com/apache/lucene/pull/16357)

> **Write-up in progress.** The design note and benchmark methodology for the
> collaborative-HNSW work will be linked here.

---

## Distributed Lucene index

**Status:** in progress

A shard-native Lucene index format designed for the collaborative search above —
so the floor-sharing coordination is a first-class property of the index rather
than a layer bolted on top.

- [distributed-search](https://github.com/ai-pipestream/distributed-search) — reference implementation
- [luceneutil](https://github.com/ai-pipestream/luceneutil) · [lucene-test-data](https://github.com/ai-pipestream/lucene-test-data) — benchmarking harness and fixtures

---

## Short-lived pipelines

**Status:** vision, with working components

The broader frame this search work sits in: pipelines that are assembled per
research question and deleted at the answer — schema, embeddings, distributed
retrieval, reranking, and NLP annotation composed as data, with an assistant
that proposes and the code that executes.

- [Short-lived pipelines: a research assistant for documents](ephemeral-pipelines.md)

---

## Related engineering notes

- [The Log-Structured Data Flow Engine](log-structured-data-flow-whitepaper.md) —
  the graph-theory framing behind the pipeline that produces the vectors we index.
