# Short-lived pipelines: a research assistant for documents

**Status:** vision, with working components · **Components:** [ProtoMolt](https://github.com/ai-pipestream/protomolt) · [distributed-search](https://github.com/ai-pipestream/distributed-search) · [Apache Lucene PR #16357](https://github.com/apache/lucene/pull/16357) · [OpenNLP research branch map](https://github.com/ai-pipestream/opennlp/blob/kristian-3.x-features/RESEARCH_BRANCHES.md)

The standard approach to document-heavy research is the *god pipeline*: one
permanent ingestion platform that every question must be shaped to fit. Most
research questions do not deserve a permanent platform. They deserve a
pipeline that is assembled for the question, used until the answer is found,
and deleted.

That is the product we are building: a research assistant that takes a stack
of documents, assembles a purpose-built pipeline around them — schema,
embeddings, retrieval, reranking, NLP annotation — and tears it down
afterwards. The wedges today are not model quality; they are integration,
document-mapping shape, and the distance between NLP tooling and LLM tooling.
Those are the wedges this architecture removes, and most of its components
already exist and are measured.

## The pieces

**ProtoMolt is the runtime.** A modular Java toolkit for Protocol Buffers:
descriptor registry, validation, mapping and CEL, an action catalog exposed
as an MCP server, and a chain manager that composes gRPC calls into
type-checked, named, stored pipelines. A pipeline in this world is *data* —
a chain definition — not a deployment. Creating one is cheap, and deleting
one is deleting a record.

**A projection standard for protobufs.** A mapping is a plain `.proto` file:
a normal message declaration plus field options binding each field to a
source path or a CEL expression over one or more source types. It is valid
proto — any `protoc` parses it — and it is self-describing, versionable in
the registry, and executable by the existing mapper. Two datasets with
incompatible schemas project into one shared shape, which is what makes
cross-source search and joins possible without per-pair glue code.

**Capability interfaces, certified before mixing.** Embedding, reranking,
and search sit behind small plain-Java service interfaces with multiple
backends: in-process static embeddings, OpenVINO Model Server, and Text
Embeddings Inference today; Triton comes free via the KServe v2 protocol
they all share. Before two backends may be mixed for the same model, an
equivalence harness must certify the pair — vector cosine and retrieval
overlap for embedders, Kendall tau for rerankers — and the harness ships
negative controls so the gate is proven to close. A backend that has not
passed runs pinned; one that has passed joins the pool.

**Distributed search with a shared floor.** The retrieval layer is the
collaborative kNN work on our research page: shards share a running floor on
the global top-k cutoff, so a shard that cannot beat it stops early. The
measured economics are why this architecture tolerates *deep* candidate
sets — and deep retrieval plus a cross-encoder rerank head is the quality
spine of the research pipeline.

**The document map.** Document understanding comes from our Apache OpenNLP
work: a typed document container with layered annotations — tokens,
sentences, entities, embeddings, places, numbers — produced by pure-Java
annotators that benchmark well ahead of their Python equivalents. The
annotated document projects through the mapper into the index, so "the
pipeline understands the documents" is a property of data, not of prompt
engineering.

**Catalogs as context.** DataHub and Gravitino integrate as schema and
dataset sources, exposed to the assistant as browsable MCP resources. "What
data is available" costs the assistant no tool calls to answer.

## The assistant, and its limits

The LLM in this system is a component behind the same interfaces as
everything else, not the mechanism. It composes chains, drafts projection
mappings, and proposes pipelines; the code executes them. Anything it
produces lands behind the same gates as hand-written work — type-checks,
certification runs, unit tests — because an assistant whose output is not
verified is a liability, not a feature. Experiment and deployment are the
same artifact at two lifecycle stages: a chain you run once, and a chain you
promote. Human interaction is the design, not the fallback.

## What is measured vs what is in flight

Measured today: the first certified cross-runtime embedding pair (the same
transformer model served by two engines, cosine 1.000000 and identical
top-k sets, admitted to the shared pool by the harness); the static
embedding path at roughly 13x the Python reference implementation's
single-thread throughput; and the shared-floor collector's visit reductions
on a 247M-vector corpus (see the research overview for tables).

In flight: the projection-standard mapping proto as a first-class standard,
catalog integrations as MCP resources, the ephemeral collection lifecycle
(create → answer → delete), and the end-to-end demo that ties them together.
