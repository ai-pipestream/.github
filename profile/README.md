# Pipestream AI

Open-source infrastructure for semantic search and document parsing. A small group
of engineers building distributed search, document processing, and the Protobuf/gRPC
tooling that connects them — and sending the fixes upstream. Everything here is
Apache-2.0 or MIT licensed.

## Search

**[distributed-search](https://github.com/ai-pipestream/distributed-search)** —
A gRPC distributed semantic search engine with collaborative HNSW nearest-neighbour
search across shards. The usual distributed contract makes every shard search to
full depth while a coordinator merges and discards most of the results; here the
shards *collaborate* during the search, sharing a running bound so each can stop
chasing candidates the rest of the cluster has already beaten. The core mechanism
is being upstreamed to Apache Lucene as a sandbox collector.
[Research write-up →](https://pipestream.ai/distributed-search/)

## Schema and Protobuf tooling

**[protomolt](https://github.com/ai-pipestream/protomolt)** —
A modular Java toolkit for Protocol Buffers at runtime, and a schema registry whose
storage is a plain Git repository behind the Confluent protocol. Every code path
works on descriptors rather than generated classes: gather `.proto` sources from
Git, Maven, jars, or other registries; validate messages with the protovalidate
dialect (full conformance suite, 2,872 of 2,872 cases, re-scored in CI on every
push); diff schemas with a typed compatibility engine; and register versions through
compatibility-gated writes, one Git commit each. The same descriptor model projects
messages into Lucene, OpenSearch, Solr, and Microsoft Graph search, and lands them
in Apache Iceberg tables via its own Hadoop-free Parquet emitter.
[Project page →](https://pipestream.ai/protomolt/)

**[protobuf4j](https://github.com/ai-pipestream/protobuf4j)** —
A pure-Java Protobuf toolkit that runs `protoc` as WebAssembly, so there are no
native binaries to ship.

**[quarkus-buf-grpc-generator](https://github.com/ai-pipestream/quarkus-buf-grpc-generator)** —
Quarkus Mutiny gRPC stubs generated with Buf, cross-platform.

**[quarkus-grpc-zero](https://github.com/ai-pipestream/quarkus-grpc-zero)** —
JVM-only, self-contained `protoc` codegen for Quarkus projects.

## Document parsing

**gRPC document parsers** —
Four original services that turn documents into structured protobuf and stream the
results back as they parse, without writing anything to disk. Rather than force every
format through one engine, each wraps the library that already handles its format
well behind the same protobuf contract:

- **[gRParse](https://github.com/ai-pipestream/gRParse)** — OCR and layout on the GPU; also coordinates the other three
- **[grpc-libreoffice](https://github.com/ai-pipestream/grpc-libreoffice)** — renders and reads office documents through LibreOffice
- **[grPOIc](https://github.com/ai-pipestream/grPOIc)** — pulls content and metadata with Apache POI
- **[grpc-calamine](https://github.com/ai-pipestream/grpc-calamine)** — streams spreadsheets cell by cell in Rust

[Overview →](https://pipestream.ai/parsers/)

**[docling-java](https://github.com/ai-pipestream/docling-java)** —
A Java API for Docling, including a gRPC service client.

**[tika4-shaded](https://github.com/ai-pipestream/tika4-shaded)** —
An all-in-one shaded Apache Tika 4 jar for quick Java integration.

**[docling-grpc-examples](https://github.com/ai-pipestream/docling-grpc-examples)** —
Runnable multi-language examples for the Docling gRPC interface.

## Upstream contributions

We send our fixes and features back to the projects we build on, and keep the work on
public branches.

- **Apache OpenNLP** — thread safety across the toolkit, a Unicode-correct,
  offset-preserving text foundation, and the deep-learning components hardened from
  demo to production; plus proposed 3.x work (static JVM embeddings, a gazetteer/
  geocoder seam, a WordNet knowledge-base seam, CJK dictionary tokenization).
  Our branch: **[ai-pipestream/opennlp @ `kristian-3.x-features`](https://github.com/ai-pipestream/opennlp/tree/kristian-3.x-features)** · [write-up](https://pipestream.ai/opennlp/)
- **Apache Lucene** — the collaborative cross-shard HNSW collector behind
  distributed-search, upstreamed as a sandbox collector.
  Our branch: **[ai-pipestream/lucene @ `kristian-11.x-features`](https://github.com/ai-pipestream/lucene/tree/kristian-11.x-features)** · [PR #16357](https://github.com/apache/lucene/pull/16357)
- Also patches into **Apache Tika**, **Quarkus** and **Quarkiverse gRPC Zero**,
  **Micronaut gRPC**, **Apicurio Registry**, the **Jandex Gradle plugin**, and the
  **Docling / MarkItDown** ecosystems.

## Research

**[Collaborative distributed HNSW](https://pipestream.ai/#research)** —
A shared-floor kNN collector that lets HNSW shards prune against a cluster-wide bound
during search rather than after it.

**[pipestream-quic-protocol-rfc](https://github.com/ai-pipestream/pipestream-quic-protocol-rfc)** —
A draft RFC for a QUIC-based pipeline transport.

---

More at **[pipestream.ai](https://pipestream.ai)**.
