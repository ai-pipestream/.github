# [ORG_NAME] — Data Orchestration Platform
## Detailed Architecture Specification

---

## Table of Contents

1. [Business Functionality](#1-business-functionality)
2. [Requirements](#2-requirements)
3. [Target State Architecture](#3-target-state-architecture)
   - 3.1 [System Context Diagram (C4 Level 1)](#31-system-context-diagram-c4-level-1)
   - 3.2 [Container Diagram (C4 Level 2)](#32-container-diagram-c4-level-2)
   - 3.3 [Component Diagram (C4 Level 3)](#33-component-diagram-c4-level-3)
4. [Key Design Patterns](#4-key-design-patterns)
   - 4.1 [Kafka Topology and Topic Design](#41-kafka-topology-and-topic-design)
   - 4.2 [Claim-Check Pattern and Document Hydration](#42-claim-check-pattern-and-document-hydration)
   - 4.3 [DAG Nodes, Edges, and CEL Routing](#43-dag-nodes-edges-and-cel-routing)
   - 4.4 [Frontend Application](#44-frontend-application)
5. [Architectural Pillars](#5-architectural-pillars)
   - 5.1 [Security and Compliance](#51-security-and-compliance)
   - 5.2 [Resilience and Reliability](#52-resilience-and-reliability)
   - 5.3 [Performance and Efficiency](#53-performance-and-efficiency)
   - 5.4 [Operational Excellence](#54-operational-excellence)
   - 5.5 [Cost Optimization](#55-cost-optimization)

---

## 1. Business Functionality

### 1.1 Platform Overview

The [ORG_NAME] Data Orchestration Platform is an enterprise document ingestion, AI-enriched processing, and semantic search system. It enables organizations to ingest documents and semi-structured data from heterogeneous sources, route them through configurable DAG-based processing pipelines, and deliver enriched output to multiple downstream sinks — including hybrid text+vector search indices, analytics platforms, and human-in-the-loop review workflows.

The platform treats data as **semi-structured and continuously enrichable**. Documents enter with varying levels of metadata — from bare file blobs to richly annotated records with external governance attributes and access control lists. At every stage of the pipeline, metadata can be decorated, transformed, and validated. The system preserves provenance and ownership context end-to-end so that downstream consumers inherit the security posture of the source.

### 1.2 Core Value Propositions

**Configurable DAG-Based Processing.**
Pipelines are defined as directed acyclic graphs (DAGs) where each node represents a processing step — parsing, chunking, embedding, enrichment, or sinking. Edges between nodes carry CEL (Common Expression Language) conditions for dynamic routing. An administrator designs pipelines visually through a DAG editor backed by Apicurio-managed schemas, and the engine executes them with full tracing and dead-letter handling.

**Combinatorial Semantic Experimentation.**
The DAG topology supports fan-out natively. A single parsed document can be routed to multiple chunking strategies (token-based, sentence-based, semantic) and each chunk set can be routed to multiple embedding models (e.g., MiniLM-L6, BGE-large, domain-specific fine-tunes). If a pipeline deploys 2 chunkers and 3 embedding models, the result is 6 independent semantic result sets per document — enabling rigorous A/B testing and quality comparison across retrieval strategies without reprocessing source data.

**Multi-Tenancy and Isolation.**
A single platform instance serves multiple accounts. Each account manages its own data sources, pipeline graphs, and output indices. Document IDs are deterministically derived and prefixed with the datasource identifier, ensuring collision-free isolation. Tier 1 configuration (global defaults) and Tier 2 configuration (per-node overrides) provide layered control.

**Privacy-First Processing (Right-to-Forget).**
Documents can be processed entirely in-memory via gRPC without touching persistent storage. This RTF (Right-to-Forget) mode satisfies strict data residency and privacy mandates — the binary payload is parsed, chunked, embedded, and indexed without ever being written to S3 or PostgreSQL.

**Metadata Integration and ACL Preservation.**
The platform integrates with external metadata governance systems at the intake and processing layers. Document-level access control lists (ACLs) sourced from upstream systems flow through the entire pipeline as first-class metadata. At query time, a proxy service enforces these ACLs against the caller's identity, ensuring users see only what they are authorized to access.

**Polyglot Module Ecosystem.**
All inter-service communication uses gRPC with Protobuf contracts published to Apicurio Registry. Processing modules can be implemented in any language — Java, Python, Go, Rust — as long as they conform to the `ModuleProcessRequest`/`ModuleProcessResponse` contract. The proxy-module pattern provides a sidecar that inherits the platform's control plane (health checks, registration, configuration injection, tracing) so that a bare processing function can be wrapped and deployed without platform-specific code.

**Multiple Sink Targets.**
OpenSearch is one output option, not the only one. The platform's sink abstraction allows any number of output destinations: search indices, data warehouses, notification systems, or human-in-the-loop review queues. Each sink is a module node in the DAG and can receive the same document independently.

**Schema-Governed Configuration.**
Apicurio Registry serves triple duty: (1) Protobuf schema registry for Kafka message validation, (2) API contract registry for gRPC service interfaces, and (3) module configuration schema store. Module JSON config schemas drive a generated frontend (JSON Forms) for the DAG editor, ensuring that pipeline configuration is always valid against the module's actual capabilities.

**Kafka-Connect Compatible Connectors.**
The connector layer exposes a Kafka Connect-compatible interface, enabling integration with the broad ecosystem of existing Kafka Connect source connectors for databases, SaaS APIs, file systems, and streaming sources.

**Frontend Analytics.**
The search interface integrates with Matomo for usage analytics — tracking query patterns, click-through rates, and result relevance. This telemetry feeds back into pipeline tuning decisions and semantic model selection.

### 1.3 Key Data Flows

```
External Sources (S3, Databases, APIs, File Systems)
    │
    ▼
┌─────────────────────────────────────────────┐
│  Connector Layer (Kafka-Connect compatible) │
│  S3 Connector · File Crawler · Custom       │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│  Intake Gate (connector-intake-service)     │
│  • API key validation (OKTA-backed)         │
│  • Doc ID derivation (deterministic)        │
│  • Tier 1 config resolution                 │
│  • ACL + metadata enrichment                │
│  • RTF vs. persistent path decision         │
└────────┬──────────────────┬─────────────────┘
         │ Path A           │ Path B (RTF)
         ▼                  │
┌──────────────────┐        │
│ Repository Service│        │
│ (S3 + Aurora PG) │        │
└────────┬─────────┘        │
         │                  │
         ▼                  ▼
┌─────────────────────────────────────────────┐
│  PipeStream Engine (DAG Orchestrator)       │
│  • Graph lookup + caching                   │
│  • Node execution: Hydrate → Filter (CEL)  │
│    → Pre-Map → Module Call → Post-Map      │
│    → Route (CEL edges)                      │
│  • DLQ + quarantine on failure              │
└────────┬────────────────────────────────────┘
         │ Fan-out to module nodes
         ▼
┌─────────────────────────────────────────────┐
│  Processing Modules (gRPC, any language)    │
│  Parser (Tika + Docling) · Chunker(s)      │
│  Embedder(s) · Custom Enrichment           │
└────────┬────────────────────────────────────┘
         │ Fan-out to sink nodes
         ▼
┌─────────────────────────────────────────────┐
│  Sink Modules                               │
│  OpenSearch · Data Warehouse · HITL Review  │
│  Notification · Analytics Export            │
└─────────────────────────────────────────────┘
```

#### Data Flow Diagram — End-to-End Ingestion Pipeline (draw.io CSV)

```csv
# label: %name%<br><i style="font-size:9px">%desc%</i>
# style: shape=%shape%;fillColor=%fill%;strokeColor=%stroke%;fontColor=#333333;fontSize=11;html=1;whiteSpace=wrap;rounded=1;arcSize=10;
# namespace: df-e2e-
# connect: {"from":"id", "to":"refs", "style":"curved=1;endArrow=block;endFill=1;strokeColor=#666666;fontSize=9;", "label":"%label%"}
# width: 180
# height: 70
# padding: 15
# ignore: id,fill,stroke,refs,shape,desc,label
# layout: horizontalflow
## ---
id,name,desc,shape,fill,stroke,refs,label
EXT,External Sources,"S3, databases, APIs, file systems",rectangle,#999999,#666666,CONN,Raw documents
CONN,Connector Layer,"S3 Connector, File Crawler, Kafka-Connect",rectangle,#438DD5,#3C7FC0,KAFKA_CRAWL,Crawl events
KAFKA_CRAWL,Kafka (crawl topics),Event stream,cylinder,#FF8C00,#CC7000,INTAKE,CrawlEvent
META,Metadata Governance,External ACL + metadata,rectangle,#999999,#666666,INTAKE,ACLs
OKTA,Okta IdP,Connector identity,rectangle,#999999,#666666,INTAKE,Token validation
INTAKE,Intake Gate,"API key validation, Doc ID derivation, Tier 1 config, ACL enrichment",rectangle,#438DD5,#3C7FC0,"REPO;ENGINE_RTF",Routes by persistence mode
REPO,Repository Service,"S3 blobs + Aurora PG metadata (Path A: Persistent)",rectangle,#438DD5,#3C7FC0,KAFKA_ENG,DocumentReference
ENGINE_RTF,Engine (RTF Direct),"In-memory gRPC handoff (Path B: Right-to-Forget)",rectangle,#438DD5,#3C7FC0,ENGINE,PipeDoc (inline)
KAFKA_ENG,Kafka (intake topics),Durable handoff,cylinder,#FF8C00,#CC7000,SIDECAR,IntakeRepoEvent
SIDECAR,Kafka Sidecar,Consumer + hydrator,rectangle,#438DD5,#3C7FC0,ENGINE,IntakeHandoff (gRPC)
ENGINE,PipeStream Engine,"DAG orchestration: Hydrate → Filter → Map → Call → Route",hexagon,#438DD5,#3C7FC0,"PARSER;CHUNKER;EMBEDDER",ModuleProcessRequest
PARSER,Parser,"Tika + Docling [GPU]",rectangle,#085B1D,#064516,ENGINE_P,Parsed metadata
ENGINE_P,Engine (post-parse),Post-mapping + routing,hexagon,#438DD5,#3C7FC0,"CHUNKER",Routes to chunkers
CHUNKER,Chunker(s),"Token, sentence, semantic",rectangle,#085B1D,#064516,ENGINE_C,Chunk sets
ENGINE_C,Engine (post-chunk),Post-mapping + routing,hexagon,#438DD5,#3C7FC0,"EMBEDDER",Routes to embedders
EMBEDDER,Embedder(s),"DJL Serving [GPU]",rectangle,#085B1D,#064516,ENGINE_E,Vectors
ENGINE_E,Engine (post-embed),Post-mapping + routing,hexagon,#438DD5,#3C7FC0,"SINK_OS;SINK_HITL;SINK_OTHER",Routes to sinks
SINK_OS,OpenSearch Sink,Hybrid text+vector index,rectangle,#085B1D,#064516,OPENSEARCH,Index document
SINK_HITL,HITL Sink,Human-in-the-loop review,rectangle,#085B1D,#064516,,Review queue
SINK_OTHER,Custom Sink,"Data warehouse, notifications, etc.",rectangle,#085B1D,#064516,,Export
OPENSEARCH,AWS OpenSearch,Managed search cluster,cylinder,#FF8C00,#CC7000,SEARCH_PROXY,Queries
SEARCH_PROXY,Search Proxy,ACL-enforcing query gateway,rectangle,#438DD5,#3C7FC0,,Filtered results
DLQ,Dead Letter Queue,Failed documents,cylinder,#CC0000,#990000,,
```

#### Data Flow Diagram — Combinatorial Semantic Fan-Out (draw.io CSV)

This diagram illustrates how 2 chunkers × 3 embedding models produce 6 independent semantic result sets.

```csv
# label: %name%<br><i style="font-size:9px">%desc%</i>
# style: shape=%shape%;fillColor=%fill%;strokeColor=%stroke%;fontColor=#333333;fontSize=11;html=1;whiteSpace=wrap;rounded=1;arcSize=10;
# namespace: df-fanout-
# connect: {"from":"id", "to":"refs", "style":"curved=1;endArrow=block;endFill=1;strokeColor=%color%;fontSize=9;", "label":"%label%"}
# width: 170
# height: 65
# padding: 15
# ignore: id,fill,stroke,refs,shape,desc,label,color
# layout: horizontalflow
## ---
id,name,desc,shape,fill,stroke,color,refs,label
PARSED,Parsed Document,Output from Parser module,rectangle,#438DD5,#3C7FC0,#666666,"CHUNK_TOK;CHUNK_SEM",Fan-out (CEL edge)
CHUNK_TOK,Chunker A,Token-based (512 tokens / 50 overlap),rectangle,#085B1D,#064516,#2E7D32,"EMBED_MINI;EMBED_BGE;EMBED_CUSTOM",Chunk set A
CHUNK_SEM,Chunker B,Sentence-based (NLP boundaries),rectangle,#085B1D,#064516,#1565C0,"EMBED_MINI_B;EMBED_BGE_B;EMBED_CUSTOM_B",Chunk set B
EMBED_MINI,Embedder: MiniLM,MiniLM-L6-v2 (384d),rectangle,#085B1D,#064516,#2E7D32,SET1,Set 1: tok+mini
EMBED_BGE,Embedder: BGE,BGE-large (1024d),rectangle,#085B1D,#064516,#2E7D32,SET2,Set 2: tok+bge
EMBED_CUSTOM,Embedder: Custom,Domain fine-tune (768d),rectangle,#085B1D,#064516,#2E7D32,SET3,Set 3: tok+custom
EMBED_MINI_B,Embedder: MiniLM,MiniLM-L6-v2 (384d),rectangle,#085B1D,#064516,#1565C0,SET4,Set 4: sent+mini
EMBED_BGE_B,Embedder: BGE,BGE-large (1024d),rectangle,#085B1D,#064516,#1565C0,SET5,Set 5: sent+bge
EMBED_CUSTOM_B,Embedder: Custom,Domain fine-tune (768d),rectangle,#085B1D,#064516,#1565C0,SET6,Set 6: sent+custom
SET1,Result Set 1,tok+mini (384d),cylinder,#FF8C00,#CC7000,#2E7D32,OS,Index
SET2,Result Set 2,tok+bge (1024d),cylinder,#FF8C00,#CC7000,#2E7D32,OS,Index
SET3,Result Set 3,tok+custom (768d),cylinder,#FF8C00,#CC7000,#2E7D32,OS,Index
SET4,Result Set 4,sent+mini (384d),cylinder,#FF8C00,#CC7000,#1565C0,OS,Index
SET5,Result Set 5,sent+bge (1024d),cylinder,#FF8C00,#CC7000,#1565C0,OS,Index
SET6,Result Set 6,sent+custom (768d),cylinder,#FF8C00,#CC7000,#1565C0,OS,Index
OS,OpenSearch,6 independent semantic indices for A/B comparison,cylinder,#FF8C00,#CC7000,#666666,,
```

#### Data Flow Diagram — Engine Node Processing Sequence (draw.io CSV)

This diagram details the internal processing steps within a single engine node execution.

```csv
# label: %name%<br><i style="font-size:9px">%desc%</i>
# style: shape=%shape%;fillColor=%fill%;strokeColor=%stroke%;fontColor=#333333;fontSize=11;html=1;whiteSpace=wrap;rounded=1;arcSize=10;
# namespace: df-node-
# connect: {"from":"id", "to":"refs", "style":"curved=1;endArrow=block;endFill=1;strokeColor=#666666;fontSize=9;", "label":"%label%"}
# width: 170
# height: 65
# padding: 15
# ignore: id,fill,stroke,refs,shape,desc,label
# layout: horizontalflow
## ---
id,name,desc,shape,fill,stroke,refs,label
INPUT,PipeStream Arrives,From Kafka sidecar or gRPC,rectangle,#999999,#666666,HYDRATE,
HYDRATE,1. Hydration,"Fetch from Repository: L1 (metadata .pb) or L2 (blob .bin)",rectangle,#438DD5,#3C7FC0,"FILTER;REPO",Hydrated doc
REPO,Repository Service,S3 + Aurora PG,cylinder,#FF8C00,#CC7000,,
FILTER,2. CEL Filter,"Evaluate filter_conditions[]; skip node if any fail",rhombus,#FFF2CC,#D6B656,"PREMAP;SKIP",Pass / Skip
SKIP,Node Skipped,Document forwarded unmodified,rectangle,#E8E8E8,#CCCCCC,ROUTE,
PREMAP,3. Pre-Mapping,"Apply ProcessingMapping[] (DIRECT, TRANSFORM, CEL)",rectangle,#438DD5,#3C7FC0,MODCALL,Transformed doc
MODCALL,4. Module Call,"gRPC to remote module; retry 3x with exponential backoff",rectangle,#085B1D,#064516,"POSTMAP;DLQ",Response
DLQ,DLQ Service,"Poison → DLQ topic → Quarantine (after retry exhaustion)",rectangle,#CC0000,#990000,,
POSTMAP,5. Post-Mapping,"Apply ProcessingMapping[] to module response",rectangle,#438DD5,#3C7FC0,META,Normalized doc
META,6. Metadata Update,"Record StepExecutionRecord; increment hop_count",rectangle,#438DD5,#3C7FC0,ROUTE,
ROUTE,7. CEL Routing,"Evaluate GraphEdge.condition for each outgoing edge",rhombus,#FFF2CC,#D6B656,"NEXT_GRPC;NEXT_KAFKA",Priority-ordered
NEXT_GRPC,Dispatch (gRPC),Direct in-memory handoff to next node,rectangle,#438DD5,#3C7FC0,,
NEXT_KAFKA,Dispatch (Kafka),"Dehydrate + publish to topic (durable, cross-cluster)",rectangle,#438DD5,#3C7FC0,,
```

---

## 2. Requirements

### 2.1 Functional Requirements

| ID | Requirement | Description |
|----|-------------|-------------|
| FR-01 | Multi-source ingestion | Accept documents from S3 buckets, file systems, databases, and REST/gRPC APIs via Kafka Connect-compatible connectors |
| FR-02 | Deterministic document identity | Derive stable document IDs using a priority algorithm (client-provided → source doc ID → URI → path) prefixed by datasource ID |
| FR-03 | Configurable DAG pipelines | Define processing pipelines as directed acyclic graphs with CEL-based routing, filtering, and field mapping |
| FR-04 | Multi-format parsing | Parse 100+ document formats (PDF, Office, email, images, HTML, EPUB, archives) via Apache Tika and Docling |
| FR-05 | Configurable chunking | Support multiple chunking strategies (token, sentence, character, semantic) with configurable size and overlap |
| FR-06 | Multi-model embedding | Generate vector embeddings from multiple models concurrently, producing combinatorial semantic result sets |
| FR-07 | Hybrid search | Index documents into OpenSearch with both full-text and k-NN vector search capabilities |
| FR-08 | ACL-aware search | Preserve source-system ACLs through the pipeline and enforce them at query time via a proxy service |
| FR-09 | Right-to-Forget (RTF) | Process documents entirely in-memory without persistent storage for privacy-sensitive workloads |
| FR-10 | Multi-tenancy | Isolate accounts, datasources, pipelines, and indices per tenant |
| FR-11 | Schema governance | Manage all Protobuf, gRPC, and module configuration schemas through Apicurio Registry |
| FR-12 | Human-in-the-loop sinks | Route documents to review queues for manual validation before final indexing |
| FR-13 | Metadata enrichment | Integrate with external metadata governance systems to decorate documents at intake and processing layers |
| FR-14 | Pipeline analytics | Track processing throughput, error rates, and module performance via Prometheus/Grafana |
| FR-15 | Search analytics | Integrate Matomo for query-level usage analytics (click-through, relevance tracking) |
| FR-16 | Visual pipeline editor | Provide a DAG drawing tool backed by Apicurio-managed module schemas and JSON Forms-generated configuration UI |

### 2.2 Non-Functional Requirements

| ID | Requirement | Target |
|----|-------------|--------|
| NFR-01 | Throughput | 1,000,000 documents/hour sustained ingestion and processing |
| NFR-02 | Processing latency | p95 end-to-end (intake → indexed) under 15 minutes |
| NFR-03 | Availability SLA | 99.9% uptime; 4-hour maximum recovery window for conservative outage scenarios |
| NFR-04 | RPO (Recovery Point Objective) | 1 hour — Kafka retains 30 days of messages; Aurora continuous backup with point-in-time recovery; S3 versioning enabled |
| NFR-05 | RTO (Recovery Target Objective) | 2 hours — Fargate tasks auto-recover; MSK brokers self-heal; Aurora failover is automatic (~30s); reprocessing from Kafka offsets covers the gap |
| NFR-06 | Data retention (Kafka) | 30-day message retention across all topics; periodic full-recall reprocessing for index rebuild |
| NFR-07 | Data retention (S3) | Indefinite with lifecycle policies for cost tiering (Standard → IA → Glacier) |
| NFR-08 | Scalability | Horizontal scaling of all Fargate services; GPU autoscaling for parser and embedder workloads |
| NFR-09 | Security | Zero-trust mTLS mesh; encryption at rest (KMS) and in transit (TLS 1.3); IAM policy enforcement |
| NFR-10 | Observability | Distributed tracing (OpenTelemetry), metrics (Prometheus/Grafana), log aggregation, Dynatrace APM integration |
| NFR-11 | Index rebuild | Full data recall from Kafka (30-day window) or S3 replay to rebuild clean OpenSearch indices on a scheduled basis |

---

## 3. Target State Architecture

All diagrams below use C4 modeling conventions and are provided in **draw.io CSV import format**. To import: open draw.io → Arrange → Insert → Advanced → CSV.

### 3.1 System Context Diagram (C4 Level 1)

This diagram shows the platform as a single box and its relationships with external actors and systems.

```csv
# label: %name%<br><i style="font-size:10px">%desc%</i>
# style: shape=%shape%;fillColor=%fill%;strokeColor=%stroke%;fontColor=#333333;fontSize=12;html=1;whiteSpace=wrap;rounded=1;arcSize=10;
# namespace: c4-l1-
# connect: {"from":"id", "to":"refs", "style":"curved=1;endArrow=block;endFill=1;strokeColor=#666666;fontSize=10;", "label":"%label%"}
# width: 200
# height: 100
# padding: 20
# ignore: id,fill,stroke,refs,shape,desc,label
# layout: horizontalflow
## ---
id,name,desc,shape,fill,stroke,refs,label
USR,End Users,Search and browse enriched documents,ellipse,#08427b,#073763,PLAT,Queries via Search UI
ADM,Administrators,Configure pipelines and manage accounts,ellipse,#08427b,#073763,PLAT,Manages via Admin UI
EXT_SRC,External Data Sources,"S3 buckets, file shares, databases, APIs",rectangle,#999999,#666666,PLAT,Pushes documents
META_GOV,Metadata Governance,External ACL and metadata systems,rectangle,#999999,#666666,PLAT,Provides ACLs and metadata
OKTA,Okta Identity Provider,Enterprise SSO and identity,rectangle,#999999,#666666,PLAT,Authenticates connectors
PLAT,[ORG_NAME] Platform,DAG-driven document processing and semantic search,rectangle,#438DD5,#3C7FC0,,
MATOMO,Matomo Analytics,Search usage and relevance tracking,rectangle,#999999,#666666,,
PLAT_M,[ORG_NAME] Platform,,,,,"MATOMO",Sends search analytics
```

### 3.2 Container Diagram (C4 Level 2)

This diagram expands the platform box into its constituent deployable units and their infrastructure dependencies.

```csv
# label: %name%<br><i style="font-size:10px">%desc%</i>
# style: shape=%shape%;fillColor=%fill%;strokeColor=%stroke%;fontColor=#333333;fontSize=12;html=1;whiteSpace=wrap;rounded=1;arcSize=10;
# namespace: c4-l2-
# connect: {"from":"id", "to":"refs", "style":"curved=1;endArrow=block;endFill=1;strokeColor=#666666;fontSize=9;", "label":"%label%"}
# width: 190
# height: 90
# padding: 20
# ignore: id,fill,stroke,refs,shape,desc,label
# layout: horizontalflow
## ---
id,name,desc,shape,fill,stroke,refs,label
EXT,External Sources,"S3, Files, DBs, APIs",rectangle,#999999,#666666,CONN,Documents
CONN,Connector Layer,"S3 Connector, Kafka-Connect compatible [Fargate]",rectangle,#438DD5,#3C7FC0,INTAKE,Crawl events via Kafka
OKTA,Okta IdP,Identity provider,rectangle,#999999,#666666,INTAKE,API key + identity validation
META,Metadata Governance,External ACL + metadata systems,rectangle,#999999,#666666,INTAKE,ACLs and metadata
INTAKE,Intake Gate,"connector-intake-service [Fargate]",rectangle,#438DD5,#3C7FC0,"REPO;MSK_I;ACCT",Persists docs or streams RTF
ACCT,Account Service,"account-service [Fargate]",rectangle,#438DD5,#3C7FC0,PG_ACCT,Account CRUD
CADMIN,Connector Admin,"connector-admin [Fargate]",rectangle,#438DD5,#3C7FC0,"PG_CADMIN;APICURIO",Datasource + API key mgmt
REPO,Repository Service,"repository-service [Fargate]",rectangle,#438DD5,#3C7FC0,"S3;PG_REPO",Blobs + metadata storage
SIDECAR,Kafka Sidecar,"engine-kafka-sidecar [Fargate]",rectangle,#438DD5,#3C7FC0,"MSK_E;ENGINE",Kafka-to-gRPC bridge
ENGINE,PipeStream Engine,"pipestream-engine [Fargate]",rectangle,#438DD5,#3C7FC0,"REPO_H;CONSUL_E",DAG orchestration
PLAT_REG,Platform Registration,"platform-registration-service [Fargate]",rectangle,#438DD5,#3C7FC0,"PG_PLAT;APICURIO;CONSUL_R",Service + schema registry
PARSER,Module: Parser,"Tika + Docling [EC2 GPU / Fargate]",rectangle,#085B1D,#064516,,Document parsing
CHUNKER,Module: Chunker,"module-chunker [Fargate]",rectangle,#085B1D,#064516,,Text chunking
EMBEDDER,Module: Embedder,"DJL Serving proxy [EC2 GPU]",rectangle,#085B1D,#064516,,Vector embedding
DJL,DJL Serving,"ML inference server [EC2 GPU + ASG]",rectangle,#999999,#666666,,Model serving
DOCLING,Docling Serve,"ML document parsing [EC2 GPU + ASG]",rectangle,#999999,#666666,,PDF/image parsing
OS_SINK,Module: OS Sink,"module-opensearch-sink [Fargate]",rectangle,#085B1D,#064516,OS_MGR,Index documents
OS_MGR,OpenSearch Manager,"opensearch-manager [Fargate]",rectangle,#438DD5,#3C7FC0,OPENSEARCH,Index + mapping management
SEARCH_PROXY,Search Proxy,"ACL-enforcing query proxy [Fargate]",rectangle,#438DD5,#3C7FC0,OPENSEARCH,ACL-filtered queries
ALB,AWS ALB,Application Load Balancer (TLS termination),rectangle,#E8E8E8,#CCCCCC,"INTAKE;SEARCH_PROXY;PLAT_REG",Routes external traffic
MSK_I,AWS MSK,Managed Kafka (intake topics),cylinder,#FF8C00,#CC7000,,
MSK_E,AWS MSK,Managed Kafka (engine topics),cylinder,#FF8C00,#CC7000,,
S3,AWS S3,Document blob storage (SSE-KMS),cylinder,#FF8C00,#CC7000,,
PG_ACCT,Aurora PostgreSQL,Account DB (Multi-AZ),cylinder,#FF8C00,#CC7000,,
PG_CADMIN,Aurora PostgreSQL,Connector Admin DB (Multi-AZ),cylinder,#FF8C00,#CC7000,,
PG_REPO,Aurora PostgreSQL,Repository DB (Multi-AZ),cylinder,#FF8C00,#CC7000,,
PG_PLAT,Aurora PostgreSQL,Registration DB (Multi-AZ),cylinder,#FF8C00,#CC7000,,
OPENSEARCH,AWS OpenSearch,Managed search cluster,cylinder,#FF8C00,#CC7000,,
APICURIO,Apicurio Registry v3,"Schema + API + config registry [Fargate]",rectangle,#438DD5,#3C7FC0,,
CONSUL_E,Consul Cluster,"Service discovery + mTLS [EC2 ASG, 3-node]",rectangle,#E8E8E8,#CCCCCC,,
CONSUL_R,Consul Cluster,Service discovery + mTLS,rectangle,#E8E8E8,#CCCCCC,,
REPO_H,Repository Service,Hydration calls,rectangle,#438DD5,#3C7FC0,,
KMS,AWS KMS,Encryption key management,rectangle,#FF8C00,#CC7000,,
SECRETS,AWS Secrets Manager,Credential storage,rectangle,#FF8C00,#CC7000,,
PROMETHEUS,Prometheus + Grafana,Metrics and dashboards,rectangle,#E8E8E8,#CCCCCC,,
DYNATRACE,Dynatrace,APM and distributed tracing,rectangle,#E8E8E8,#CCCCCC,,
MATOMO,Matomo,Search analytics,rectangle,#E8E8E8,#CCCCCC,,
```

**Legend:**
- Blue (#438DD5): Platform-owned services on AWS Fargate
- Green (#085B1D): Processing modules (gRPC, polyglot)
- Orange (#FF8C00): AWS managed data services
- Gray (#E8E8E8/#999999): Infrastructure and external systems

**AWS MSK Configuration:**
- 6 brokers across 3 Availability Zones
- Replication factor: 2
- 30-day retention on all topics
- Protobuf serialization with Apicurio schema validation

**Aurora PostgreSQL:**
- Separate cluster per service (account, connector-admin, repository, registration, engine)
- Multi-AZ deployment for high availability
- Continuous backup with point-in-time recovery

**OpenSearch:**
- AWS Managed OpenSearch Service
- Initial capacity: ~100 GB scaling to 200 GB+
- 7 primary shards, 3 replicas per index
- OKTA SAML integration for dashboard access
- k-NN plugin enabled (Lucene engine, cosine similarity)

### 3.3 Component Diagram (C4 Level 3)

This diagram shows the internal components of the core services and how they interact during document processing.

#### 3.3.1 Intake and Connector Components

```csv
# label: %name%<br><i style="font-size:10px">%desc%</i>
# style: shape=%shape%;fillColor=%fill%;strokeColor=%stroke%;fontColor=#333333;fontSize=11;html=1;whiteSpace=wrap;rounded=1;arcSize=10;
# namespace: c4-l3-intake-
# connect: {"from":"id", "to":"refs", "style":"curved=1;endArrow=block;endFill=1;strokeColor=#666666;fontSize=9;", "label":"%label%"}
# width: 180
# height: 80
# padding: 15
# ignore: id,fill,stroke,refs,shape,desc,label
# layout: horizontalflow
## ---
id,name,desc,shape,fill,stroke,refs,label
EXT_S3,External S3 Bucket,Customer data source,rectangle,#999999,#666666,S3_CRAWL,ListObjectsV2
S3_CRAWL,S3 Crawl Service,Bucket discovery + pagination,rectangle,#438DD5,#3C7FC0,S3_PUB,Discovered objects
S3_CLIENT,S3 Client Factory,Per-datasource client pool,rectangle,#438DD5,#3C7FC0,,
S3_PUB,Crawl Event Publisher,Kafka producer,rectangle,#438DD5,#3C7FC0,KAFKA_CRAWL,S3CrawlEvent
KAFKA_CRAWL,Kafka (crawl topic),Crawl event stream,cylinder,#FF8C00,#CC7000,INTAKE_SVC,Events
OKTA_IDP,Okta IdP,Identity provider,rectangle,#999999,#666666,API_KEY_VALID,Token validation
API_KEY_VALID,API Key Validator,Argon2id verification + Okta,rectangle,#438DD5,#3C7FC0,,
INTAKE_SVC,Intake Service,Document acceptance gateway,rectangle,#438DD5,#3C7FC0,"API_KEY_VALID;DOCID;TIER1;OWN",Validates + routes
DOCID,Doc ID Deriver,"Priority: client → source → URI → path",rectangle,#438DD5,#3C7FC0,,
TIER1,Tier 1 Config Resolver,Datasource default configuration,rectangle,#438DD5,#3C7FC0,CADMIN_SVC,Fetches config
CADMIN_SVC,Connector Admin Service,Datasource + config management,rectangle,#438DD5,#3C7FC0,"PG_CA;APICURIO_R",CRUD + schema
PG_CA,Aurora PostgreSQL,Connector admin database,cylinder,#FF8C00,#CC7000,,
OWN,Ownership Enrichment,ACL + metadata decoration,rectangle,#438DD5,#3C7FC0,META_EXT,Resolves context
META_EXT,Metadata Governance,External ACL + metadata system,rectangle,#999999,#666666,,
PERSIST,Persistence Router,Path A (S3) or Path B (RTF),rectangle,#438DD5,#3C7FC0,"REPO_SVC;ENGINE_DIRECT",Routes by config
REPO_SVC,Repository Service,S3 blobs + PG metadata,rectangle,#438DD5,#3C7FC0,"S3_STORE;PG_REPO",Stores document
S3_STORE,AWS S3,SSE-KMS encrypted blobs,cylinder,#FF8C00,#CC7000,,
PG_REPO,Aurora PostgreSQL,Repository metadata,cylinder,#FF8C00,#CC7000,,
ENGINE_DIRECT,Engine (gRPC direct),RTF in-memory handoff,rectangle,#438DD5,#3C7FC0,,
APICURIO_R,Apicurio Registry,Schema validation,rectangle,#438DD5,#3C7FC0,,
```

#### 3.3.2 Engine Orchestration Components

```csv
# label: %name%<br><i style="font-size:10px">%desc%</i>
# style: shape=%shape%;fillColor=%fill%;strokeColor=%stroke%;fontColor=#333333;fontSize=11;html=1;whiteSpace=wrap;rounded=1;arcSize=10;
# namespace: c4-l3-engine-
# connect: {"from":"id", "to":"refs", "style":"curved=1;endArrow=block;endFill=1;strokeColor=#666666;fontSize=9;", "label":"%label%"}
# width: 180
# height: 80
# padding: 15
# ignore: id,fill,stroke,refs,shape,desc,label
# layout: horizontalflow
## ---
id,name,desc,shape,fill,stroke,refs,label
KAFKA_IN,Kafka Sidecar,Kafka consumer + hydrator,rectangle,#438DD5,#3C7FC0,ENGINE_SVC,IntakeHandoff (gRPC)
ENGINE_SVC,Engine gRPC Service,"ProcessNode, IntakeHandoff",rectangle,#438DD5,#3C7FC0,"GRAPH_CACHE;HYDRATION",Request dispatch
GRAPH_CACHE,Graph Cache,In-memory pipeline graph lookup,rectangle,#438DD5,#3C7FC0,PG_ENG,Versioned graph fetch
PG_ENG,Aurora PostgreSQL,Engine DB (JSONB graph storage),cylinder,#FF8C00,#CC7000,,
HYDRATION,Hydration Service,"L1 metadata, L2 blob fetch",rectangle,#438DD5,#3C7FC0,REPO_CLIENT,On-demand fetch
REPO_CLIENT,Repository Client,gRPC client to repository-service,rectangle,#438DD5,#3C7FC0,,
CEL_FILTER,CEL Filter Evaluator,Node skip/pass decisions,rectangle,#438DD5,#3C7FC0,,
PRE_MAP,Pre-Mapping Service,Field transforms before module call,rectangle,#438DD5,#3C7FC0,FIELD_MAP,
FIELD_MAP,Field Mapping Engine,CEL-based field transformations,rectangle,#438DD5,#3C7FC0,,
MODULE_CALL,Module Caller,gRPC call with retry + backoff,rectangle,#438DD5,#3C7FC0,"MOD_PARSER;MOD_CHUNK;MOD_EMBED;MOD_SINK",Calls modules
POST_MAP,Post-Mapping Service,Field transforms after module call,rectangle,#438DD5,#3C7FC0,FIELD_MAP_P,
FIELD_MAP_P,Field Mapping Engine,Post-processing transforms,rectangle,#438DD5,#3C7FC0,,
ROUTING,Routing Service,CEL edge evaluation + dispatch,rectangle,#438DD5,#3C7FC0,"DISPATCH_GRPC;DISPATCH_KAFKA",Next-hop routing
DISPATCH_GRPC,gRPC Dispatch,Direct low-latency handoff,rectangle,#438DD5,#3C7FC0,,
DISPATCH_KAFKA,Kafka Dispatch,Durable topic handoff,rectangle,#438DD5,#3C7FC0,MSK_OUT,Publishes PipeStream
MSK_OUT,AWS MSK,Engine output topics,cylinder,#FF8C00,#CC7000,,
DLQ_SVC,DLQ Service,"Dead letter queue, quarantine, retry",rectangle,#438DD5,#3C7FC0,MSK_DLQ,Poison messages
MSK_DLQ,Kafka DLQ Topics,Dead letter + quarantine topics,cylinder,#FF8C00,#CC7000,,
CONSUL,Consul,Module discovery + mTLS,rectangle,#E8E8E8,#CCCCCC,,
STORK,SmallRye Stork,Client-side load balancing,rectangle,#438DD5,#3C7FC0,CONSUL,Service lookup
MOD_PARSER,Parser Module,Tika + Docling (gRPC),rectangle,#085B1D,#064516,,
MOD_CHUNK,Chunker Module,Token/sentence/semantic (gRPC),rectangle,#085B1D,#064516,,
MOD_EMBED,Embedder Module,DJL Serving proxy (gRPC),rectangle,#085B1D,#064516,,
MOD_SINK,Sink Modules,"OpenSearch, HITL, custom (gRPC)",rectangle,#085B1D,#064516,,
METRICS,Engine Metrics,Prometheus counters + histograms,rectangle,#E8E8E8,#CCCCCC,,
TRACING,OpenTelemetry,Distributed trace propagation,rectangle,#E8E8E8,#CCCCCC,,
```

#### 3.3.3 Processing Module Components

```csv
# label: %name%<br><i style="font-size:10px">%desc%</i>
# style: shape=%shape%;fillColor=%fill%;strokeColor=%stroke%;fontColor=#333333;fontSize=11;html=1;whiteSpace=wrap;rounded=1;arcSize=10;
# namespace: c4-l3-modules-
# connect: {"from":"id", "to":"refs", "style":"curved=1;endArrow=block;endFill=1;strokeColor=#666666;fontSize=9;", "label":"%label%"}
# width: 180
# height: 80
# padding: 15
# ignore: id,fill,stroke,refs,shape,desc,label
# layout: horizontalflow
## ---
id,name,desc,shape,fill,stroke,refs,label
ENGINE,Engine,Calls modules via gRPC,rectangle,#438DD5,#3C7FC0,"PARSER_SVC;CHUNKER_SVC;EMBED_SVC;SINK_SVC;PROXY_SVC",ModuleProcessRequest
PARSER_SVC,Parser Service,"gRPC service [EC2 GPU / Fargate]",rectangle,#085B1D,#064516,"TIKA;DOCLING_INT",Routes by format
TIKA,Apache Tika 3.2,100+ formats / 1330+ metadata fields,rectangle,#085B1D,#064516,META_BUILD,Parses document
DOCLING_INT,Docling Integration,ML-based PDF/image parsing,rectangle,#085B1D,#064516,DOCLING_SRV,gRPC to Docling
DOCLING_SRV,Docling Serve,"ML inference [EC2 GPU + ASG, SSL at ALB]",rectangle,#999999,#666666,,
META_BUILD,Metadata Builders,"18 specialized extractors (PDF, Office, Email, ...)",rectangle,#085B1D,#064516,,
CHUNKER_SVC,Chunker Service,gRPC service [Fargate],rectangle,#085B1D,#064516,CHUNK_ALG,Chunks text
CHUNK_ALG,Chunking Algorithms,"Token, sentence, character, semantic",rectangle,#085B1D,#064516,,
EMBED_SVC,Embedder Service,gRPC proxy [Fargate],rectangle,#085B1D,#064516,DJL_SRV,Proxies to DJL
DJL_SRV,DJL Serving,"ML model inference [EC2 GPU + ASG, SSL at ALB]",rectangle,#999999,#666666,,
SINK_SVC,OpenSearch Sink,gRPC service [Fargate],rectangle,#085B1D,#064516,OS_MGR,Indexes documents
OS_MGR,OpenSearch Manager,Index + field mapping management,rectangle,#438DD5,#3C7FC0,OPENSEARCH,Bulk index
OPENSEARCH,AWS OpenSearch,"Managed cluster (7 shards, 3 replicas)",cylinder,#FF8C00,#CC7000,,
PROXY_SVC,Proxy Module,Sidecar wrapper for custom modules,rectangle,#085B1D,#064516,CUSTOM_MOD,Inherits platform controls
CUSTOM_MOD,Custom Module,"Any language (Python, Go, Rust, ...)",rectangle,#999999,#666666,,
APICURIO,Apicurio Registry,Module config schemas (JSON Forms),rectangle,#438DD5,#3C7FC0,,
CONSUL,Consul,Registration + health checks,rectangle,#E8E8E8,#CCCCCC,,
```

#### 3.3.4 Search and Query Components

```csv
# label: %name%<br><i style="font-size:10px">%desc%</i>
# style: shape=%shape%;fillColor=%fill%;strokeColor=%stroke%;fontColor=#333333;fontSize=11;html=1;whiteSpace=wrap;rounded=1;arcSize=10;
# namespace: c4-l3-search-
# connect: {"from":"id", "to":"refs", "style":"curved=1;endArrow=block;endFill=1;strokeColor=#666666;fontSize=9;", "label":"%label%"}
# width: 180
# height: 80
# padding: 15
# ignore: id,fill,stroke,refs,shape,desc,label
# layout: horizontalflow
## ---
id,name,desc,shape,fill,stroke,refs,label
USER,End User,Searches enriched documents,ellipse,#999999,#666666,SEARCH_UI,Query
SEARCH_UI,Search Frontend,"Vue 3 + Vuetify [Fargate / CDN]",rectangle,#438DD5,#3C7FC0,"SEARCH_PROXY;MATOMO_INT",Hybrid search queries
MATOMO_INT,Matomo,Search analytics tracking,rectangle,#E8E8E8,#CCCCCC,,
SEARCH_PROXY,Search Proxy Service,ACL enforcement gateway [Fargate],rectangle,#438DD5,#3C7FC0,"ACL_FILTER;OPENSEARCH_Q",Filtered queries
ACL_FILTER,ACL Filter,Identity-aware result filtering,rectangle,#438DD5,#3C7FC0,OKTA_V,Validates caller ACLs
OKTA_V,Okta / Identity,Caller identity resolution,rectangle,#999999,#666666,,
OPENSEARCH_Q,AWS OpenSearch,"Hybrid text + k-NN vector search",cylinder,#FF8C00,#CC7000,,
ADMIN,Administrator,Manages pipelines and accounts,ellipse,#999999,#666666,ADMIN_UI,Configuration
ADMIN_UI,Admin + DAG Editor,"Vue 3 + JSON Forms [Fargate / CDN]",rectangle,#438DD5,#3C7FC0,"PLAT_REG;ENGINE_CFG",Pipeline design
PLAT_REG,Platform Registration,Service + module registry,rectangle,#438DD5,#3C7FC0,"APICURIO;CONSUL",Schema + discovery
ENGINE_CFG,Engine Config API,"PipelineConfigService (gRPC)",rectangle,#438DD5,#3C7FC0,PG_ENG,Graph CRUD
PG_ENG,Aurora PostgreSQL,Pipeline graph storage,cylinder,#FF8C00,#CC7000,,
APICURIO,Apicurio Registry,Proto + JSON config schemas,rectangle,#438DD5,#3C7FC0,,
CONSUL,Consul,Service health + discovery,rectangle,#E8E8E8,#CCCCCC,,
```

---

## 4. Key Design Patterns

### 4.1 Kafka Topology and Topic Design

Kafka (AWS MSK) is the durable backbone of the platform. It serves three distinct roles: connector event delivery, engine-to-engine document handoff, and operational event streaming. Every topic uses Protobuf serialization validated against Apicurio Registry schemas.

#### 4.1.1 Topic Taxonomy

| Category | Topic Pattern | Purpose | Producer | Consumer |
|----------|--------------|---------|----------|----------|
| **Crawl Events** | `s3-crawl-events-out` | Object discovery notifications from connectors | S3 Connector | Intake Gate |
| **Intake Handoff** | `intake.<datasource_id>` | Document arrival events scoped by datasource | Intake Gate | Kafka Sidecar |
| **Repository Events** | `repository-events` | Metadata indexing notifications (CREATED/DELETED) | Repository Service | OpenSearch Manager |
| **Engine Routing** | `pipestream.<cluster>.<node_id>` | Inter-node document transport (Kafka edges) | Engine (Dispatch) | Kafka Sidecar |
| **Cross-Cluster** | `pipestream.<cluster>.bridge.<target_cluster>` | Cross-cluster document routing | Engine (Routing) | Remote Kafka Sidecar |
| **Dead Letter** | `pipestream.<cluster>.<node_id>.dlq` | Failed documents after retry exhaustion | Engine (DLQ Service) | Ops / Reprocessor |
| **Quarantine** | `pipestream.<cluster>.<node_id>.quarantine` | Poison messages beyond DLQ recovery | Engine (DLQ Service) | Manual investigation |
| **Global DLQ** | `pipestream.global.dlq` | Fallback for unroutable DLQ failures | Engine (DLQ Service) | Ops |
| **Entity Updates** | `drive-updates`, `node-updates`, `module-updates`, `pipedoc-updates`, `graph-updates` | Entity lifecycle events for search indexing | Various services | OpenSearch Manager |
| **Account Events** | `account-events` | Account lifecycle (created, inactivated, etc.) | Account Service | Downstream consumers |

#### 4.1.2 MSK Cluster Design

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Broker count | 6 (2 per AZ across 3 AZs) | Supports 1M docs/hour throughput with headroom for burst |
| Replication factor | 2 | Tolerates single-broker failure; data is not the system of record (S3 is), so RF=2 balances durability against storage cost |
| Retention | 30 days | Enables full index rebuild from Kafka replay; covers extended outage recovery |
| Partitioning strategy | By `datasource_id` | Ensures per-datasource ordering; enables parallel consumption across datasources |
| Serialization | Protobuf (binary) | Compact wire format; schema-validated via Apicurio Registry |
| Compression | LZ4 | Low CPU overhead; good compression for protobuf payloads |
| Min ISR | 1 | Allows writes during single-replica degradation (acceptable given S3 as source of truth) |

#### 4.1.3 Transport Decision: gRPC vs Kafka

Each `GraphEdge` in a pipeline DAG carries a `transport_type` field that determines how a document moves between nodes:

| Transport | When to Use | Characteristics |
|-----------|-------------|-----------------|
| **gRPC (direct)** | Tightly coupled processing chains within the same cluster (e.g., parser → chunker → embedder) | Sub-millisecond handoff; in-memory; no persistence between hops; ideal for RTF workloads; consumer must be available |
| **Kafka (messaging)** | Cross-cluster routing, horizontal fan-out, durable handoffs, decoupled scaling | Document dehydrated to `DocumentReference` and persisted to S3 before handoff; independent producer/consumer scaling; 30-day replay; survives consumer downtime |

The engine's `DispatchService` inspects the edge transport type and routes accordingly. gRPC dispatch calls `ProcessNode` directly on the target engine instance. Kafka dispatch serializes a `PipeStream` with a `DocumentReference` and publishes to the edge's configured topic.

#### 4.1.4 Kafka Topic Flow Diagram (draw.io CSV)

```csv
# label: %name%<br><i style="font-size:9px">%desc%</i>
# style: shape=%shape%;fillColor=%fill%;strokeColor=%stroke%;fontColor=#333333;fontSize=11;html=1;whiteSpace=wrap;rounded=1;arcSize=10;
# namespace: df-kafka-
# connect: {"from":"id", "to":"refs", "style":"curved=1;endArrow=block;endFill=1;strokeColor=#666666;fontSize=9;", "label":"%label%"}
# width: 180
# height: 65
# padding: 15
# ignore: id,fill,stroke,refs,shape,desc,label
# layout: horizontalflow
## ---
id,name,desc,shape,fill,stroke,refs,label
S3_CONN,S3 Connector,Crawls external buckets,rectangle,#438DD5,#3C7FC0,T_CRAWL,Publishes
T_CRAWL,s3-crawl-events-out,Crawl event topic,cylinder,#FF8C00,#CC7000,INTAKE,Consumes
INTAKE,Intake Gate,Document acceptance,rectangle,#438DD5,#3C7FC0,"T_INTAKE;REPO",Publishes
T_INTAKE,intake.<datasource_id>,Per-datasource intake topic,cylinder,#FF8C00,#CC7000,SIDECAR,Consumes
REPO,Repository Service,Blob + metadata storage,rectangle,#438DD5,#3C7FC0,T_REPO,Publishes
T_REPO,repository-events,Entity lifecycle events,cylinder,#FF8C00,#CC7000,OS_MGR,Consumes
SIDECAR,Kafka Sidecar,Kafka-to-gRPC bridge,rectangle,#438DD5,#3C7FC0,ENGINE,IntakeHandoff
ENGINE,PipeStream Engine,DAG orchestrator,hexagon,#438DD5,#3C7FC0,"T_ROUTE;T_DLQ",Publishes
T_ROUTE,pipestream.<cluster>.<node>,Inter-node routing topics,cylinder,#FF8C00,#CC7000,SIDECAR_R,Consumes
SIDECAR_R,Kafka Sidecar,Routes back to engine,rectangle,#438DD5,#3C7FC0,,ProcessNode
T_DLQ,pipestream.*.dlq,Dead letter + quarantine,cylinder,#CC0000,#990000,OPS,Alert
OS_MGR,OpenSearch Manager,Index management,rectangle,#438DD5,#3C7FC0,T_ENTITY,Consumes
T_ENTITY,"drive/node/module/graph-updates",Entity update topics,cylinder,#FF8C00,#CC7000,OS_MGR_E,Consumes
OS_MGR_E,OpenSearch Manager,Indexes entities,rectangle,#438DD5,#3C7FC0,,
ACCT,Account Service,Account lifecycle,rectangle,#438DD5,#3C7FC0,T_ACCT,Publishes
T_ACCT,account-events,Account lifecycle events,cylinder,#FF8C00,#CC7000,,
OPS,Operations,Monitoring + reprocessing,ellipse,#999999,#666666,,
```

### 4.2 Claim-Check Pattern and Document Hydration

#### 4.2.1 The Problem

A fully hydrated `PipeDoc` with blob content can be megabytes to gigabytes in size. Passing this payload through every Kafka topic and gRPC call in a multi-node DAG would be prohibitively expensive in bandwidth, memory, and latency.

#### 4.2.2 The Claim-Check Solution

The platform implements the claim-check pattern: large payloads are stored externally (S3 + Aurora PostgreSQL), and only a lightweight reference — the `DocumentReference` — travels through Kafka and between engine hops.

```
┌──────────────────────────────────────────────────────────┐
│  DocumentReference (the "claim check")                   │
│  ┌────────────────────────────────────────────────────┐  │
│  │  doc_id:           "ds_abc123.doc_7f3e"            │  │
│  │  graph_address_id: "prod.parser-v1"                │  │
│  │  account_id:       "acct_001"                      │  │
│  │  s3_key:           "acct_001/ds_abc123/doc_7f3e"   │  │
│  │  version:          3                                │  │
│  └────────────────────────────────────────────────────┘  │
│                                                          │
│  Size: ~200 bytes                                        │
│  vs. full PipeDoc with blob: potentially gigabytes       │
└──────────────────────────────────────────────────────────┘
```

The composite key `(doc_id, graph_address_id, account_id)` uniquely identifies the document's state at a specific DAG node. This means the repository stores **point-in-time snapshots** — the document after parsing is a different record than the document after chunking, enabling lineage tracking and debugging.

#### 4.2.3 Two-Level Hydration

When the engine needs to process a document at a node, the `HydrationService` fetches only what is required:

| Level | What is Fetched | File | Typical Size | When Used |
|-------|----------------|------|--------------|-----------|
| **L1 — Wrapper** | `PipeDoc` metadata envelope: `SearchMetadata`, `OwnershipContext`, `parsed_metadata` map, `structured_data` (Any), `doc_id_derivation` | `.pb` (Protobuf binary) | ~1–50 KB | Every node — needed for CEL filter evaluation, field mapping, and routing decisions |
| **L2 — Content** | Raw binary document bytes: the actual PDF, image, email, or file content | `.bin` (raw blob from S3) | KB to GB | Only at parser nodes — the engine inspects the target module's capabilities and fetches L2 only if the module requires blob access |

**Hydration is lazy and module-aware.** Before calling a module, the engine checks whether it needs blob content. A chunker operating on already-parsed text never triggers an L2 fetch. This dramatically reduces S3 GET requests and data transfer.

```
              Kafka Sidecar receives DocumentReference
                            │
                            ▼
                   ┌─────────────────┐
                   │  L1 Hydration   │
                   │  Fetch .pb from │◄──── Aurora PostgreSQL
                   │  repository     │      (metadata lookup)
                   └────────┬────────┘
                            │
                   PipeDoc with metadata,
                   SearchMetadata, ACLs,
                   parsed_metadata — but
                   BlobBag contains only
                   a storage_ref (no bytes)
                            │
                            ▼
                   ┌─────────────────┐
                   │  Is this node   │
                   │  a PARSER?      │
                   └───┬─────────┬───┘
                    Yes│         │No
                       ▼         ▼
              ┌──────────────┐  Continue with L1 data
              │ L2 Hydration │  (chunker, embedder,
              │ Fetch .bin   │◄── AWS S3   sink operate on
              │ from S3      │    (blob)   parsed text, not
              └──────┬───────┘             raw bytes)
                     │
                     ▼
              PipeDoc with full
              blob bytes inline
              (ready for Tika/Docling)
```

#### 4.2.4 Dehydration

After a node completes processing, the engine may **dehydrate** the document before the next hop:
- **Kafka edges:** the document is always dehydrated — the updated `PipeDoc` is saved to the repository (new snapshot at `graph_address_id` = current node), and only the `DocumentReference` is published to the topic
- **gRPC edges:** dehydration is optional — for RTF (Right-to-Forget) workloads, the full `PipeDoc` stays in memory and is never written to S3
- **`drop_blobs_after_parse`:** a per-datasource configuration option that discards raw binary content after parsing, reducing S3 storage for workloads where only extracted text matters

#### 4.2.5 Claim-Check Flow Diagram (draw.io CSV)

```csv
# label: %name%<br><i style="font-size:9px">%desc%</i>
# style: shape=%shape%;fillColor=%fill%;strokeColor=%stroke%;fontColor=#333333;fontSize=11;html=1;whiteSpace=wrap;rounded=1;arcSize=10;
# namespace: df-claimcheck-
# connect: {"from":"id", "to":"refs", "style":"curved=1;endArrow=block;endFill=1;strokeColor=#666666;fontSize=9;", "label":"%label%"}
# width: 175
# height: 65
# padding: 15
# ignore: id,fill,stroke,refs,shape,desc,label
# layout: horizontalflow
## ---
id,name,desc,shape,fill,stroke,refs,label
INTAKE,Intake Gate,Receives document + blob,rectangle,#438DD5,#3C7FC0,"S3_W;PG_W",Stores (claim-check)
S3_W,AWS S3,Blob storage (.bin),cylinder,#FF8C00,#CC7000,,
PG_W,Aurora PostgreSQL,Metadata storage (.pb),cylinder,#FF8C00,#CC7000,,
INTAKE_K,Intake Gate,,rectangle,#438DD5,#3C7FC0,KAFKA,Publishes DocumentReference (~200 bytes)
KAFKA,Kafka Topic,Lightweight reference only,cylinder,#FF8C00,#CC7000,SIDECAR,Consumes
SIDECAR,Kafka Sidecar,Receives claim check,rectangle,#438DD5,#3C7FC0,"L1",Hydrates
L1,L1 Hydration,Fetch .pb metadata (~1-50KB),rectangle,#438DD5,#3C7FC0,"PG_R;DECIDE",Reads
PG_R,Aurora PostgreSQL,Metadata store,cylinder,#FF8C00,#CC7000,,
DECIDE,Parser Node?,Does module need blob?,rhombus,#FFF2CC,#D6B656,"L2;ENGINE_L1",Yes / No
L2,L2 Hydration,Fetch .bin blob (KB–GB),rectangle,#438DD5,#3C7FC0,"S3_R;ENGINE_L2",Reads
S3_R,AWS S3,Blob store,cylinder,#FF8C00,#CC7000,,
ENGINE_L1,Engine (L1 only),Processes with metadata only,hexagon,#438DD5,#3C7FC0,DEHYDRATE,Chunker / Embedder / Sink
ENGINE_L2,Engine (L1+L2),Processes with full blob,hexagon,#438DD5,#3C7FC0,DEHYDRATE,Parser
DEHYDRATE,Dehydration,Save snapshot + emit reference,rectangle,#438DD5,#3C7FC0,"S3_N;PG_N;KAFKA_N",New claim check
S3_N,AWS S3,Updated blob (if changed),cylinder,#FF8C00,#CC7000,,
PG_N,Aurora PostgreSQL,Snapshot at new graph_address_id,cylinder,#FF8C00,#CC7000,,
KAFKA_N,Next Kafka Topic,DocumentReference to next node,cylinder,#FF8C00,#CC7000,,
```

### 4.3 DAG Nodes, Edges, and CEL Routing

#### 4.3.1 Pipeline Graph Structure

A `PipelineGraph` is a self-contained, versioned DAG definition stored as JSONB in Aurora PostgreSQL. Graphs are immutable snapshots — editing creates a new version, and the engine caches the active version in memory for fast lookup.

```
PipelineGraph
├── graph_id:    "graph_prod_001"
├── cluster_id:  "prod"
├── mode:        PRODUCTION (or DESIGN)
├── version:     7 (optimistic locking)
├── nodes[]:     GraphNode definitions (self-contained)
└── edges[]:     GraphEdge connections with CEL conditions
```

#### 4.3.2 Graph Nodes (`GraphNode`)

Each node represents a single processing step bound to a registered module.

| Field | Description |
|-------|-------------|
| `node_id` | Fully qualified: `{cluster_id}.{node_name}` (e.g., `prod.parser-tika`) |
| `node_type` | `CONNECTOR` (ingress), `PROCESSOR` (transform), or `SINK` (egress) |
| `module_id` | Reference to a registered `ModuleDefinition` — tells the engine which gRPC service to call |
| `custom_config` | `ProcessConfiguration` with `json_config` (Struct) and `config_params` (map) — validated against the module's JSON schema in Apicurio |
| `kafka_input_topic` | For Kafka-transport edges: the topic this node consumes from (e.g., `prod.chunker-v1`) |
| `repository_path` | Where to store point-in-time snapshots: `/clusters/{cluster_id}/nodes/{node_id}` |
| `pre_mappings[]` | Field transformations applied before the module call |
| `post_mappings[]` | Field transformations applied after the module call |
| `filter_conditions[]` | CEL expressions — all must pass for the node to execute; otherwise the document is forwarded unmodified |
| `dlq_config` | Per-node dead letter queue settings (max retries, topic override) |
| `design_config` | Design-mode settings: canvas position, simulated latency, simulated success rate, sample I/O data |

#### 4.3.3 Graph Edges (`GraphEdge`)

Edges define how documents flow between nodes, with optional conditional routing.

| Field | Description |
|-------|-------------|
| `from_node_id` / `to_node_id` | Source and target nodes |
| `condition` | CEL expression evaluated against the document — edge is followed only if true (empty = always follow) |
| `priority` | Lower value = higher priority — evaluated in order; first matching edge wins (or all matching, for fan-out) |
| `transport_type` | `MESSAGING` (Kafka) or `GRPC` (direct) |
| `kafka_topic` | Custom topic for Kafka transport (defaults to `pipestream.<cluster>.<to_node_id>`) |
| `is_cross_cluster` / `to_cluster_id` | Enables routing to nodes in a different cluster |
| `max_hops` | Loop prevention — engine drops the document if hop count exceeds this threshold |

#### 4.3.4 CEL Expression Context

CEL expressions in filters and edge conditions have access to the following variables:

| Variable | Type | Contents |
|----------|------|----------|
| `document` | `PipeDoc` | The current document: `doc_id`, `search_metadata`, `blob_bag`, `structured_data`, `parsed_metadata`, `ownership` |
| `document.search_metadata` | `SearchMetadata` | Common fields: `title`, `body`, `language`, `content_length`, `mime_type`, `tags`, `custom_fields` |
| `document.ownership` | `OwnershipContext` | `account_id`, `datasource_id`, `connector_id` |
| `stream` | `PipeStream` | The envelope: `stream_id`, `cluster_id`, `current_node_id`, `hop_count`, `processing_path` |
| `stream.metadata` | `StreamMetadata` | `source_id`, `datasource_id`, `account_id`, `entry_node_id`, `context_params` |
| `context_params` | `map<string, string>` | Arbitrary key-value pairs injected at intake for routing control |

**Example CEL expressions:**

```
# Filter: only process English documents
document.search_metadata.language == "en"

# Filter: skip documents under 1KB
document.search_metadata.content_length > 1024

# Edge condition: route PDFs to Docling, everything else to Tika
document.search_metadata.mime_type == "application/pdf"

# Edge condition: route high-priority documents to fast-path
stream.metadata.context_params["priority"] == "high"

# Edge condition: route by datasource for tenant-specific processing
document.ownership.datasource_id == "ds_legal_contracts"
```

#### 4.3.5 Field Mapping (`ProcessingMapping`)

Pre-mappings and post-mappings transform document fields before and after module calls. Five mapping types are supported:

| Type | Description | Example |
|------|-------------|---------|
| `DIRECT` | Copy field A to field B | `search_metadata.title` → `custom_fields.display_name` |
| `TRANSFORM` | Apply a transformation rule (uppercase, lowercase, proto_rules) | `search_metadata.body` → uppercase → `custom_fields.BODY` |
| `AGGREGATE` | Combine multiple source fields into one (concatenate, sum, list) | `[title, subtitle]` → concat → `display_title` |
| `SPLIT` | Split a single field by delimiter into multiple targets | `tags_csv` → split(",") → `tags[]` |
| `CEL` | Evaluate a CEL expression to compute the target value | `document.search_metadata.content_length > 10000 ? "large" : "small"` → `size_category` |

#### 4.3.6 DAG Execution Example

A typical production pipeline for document processing:

```
                        ┌──────────────┐
                        │  S3 Connector │
                        │  (CONNECTOR)  │
                        └──────┬───────┘
                               │ Kafka edge
                               ▼
                        ┌──────────────┐
                        │  Intake Gate  │
                        │  (entry node) │
                        └──────┬───────┘
                               │ Kafka edge
                               ▼
                        ┌──────────────┐
                        │  Parser Node  │  CEL filter: blob_bag != null
                        │  module: tika │  L2 hydration triggered
                        └──────┬───────┘
                               │ gRPC edge
               ┌───────────────┼───────────────┐
               │ CEL: lang=="en"               │ CEL: lang!="en"
               ▼                               ▼
        ┌──────────────┐                ┌──────────────┐
        │ Chunker A    │                │ Chunker B    │
        │ token (512)  │                │ sentence     │
        └──────┬───────┘                └──────┬───────┘
               │ gRPC edge                     │ gRPC edge
        ┌──────┴───────┐                       │
        ▼              ▼                       ▼
 ┌────────────┐ ┌────────────┐          ┌────────────┐
 │ Embedder A │ │ Embedder B │          │ Embedder A │
 │ MiniLM     │ │ BGE-large  │          │ MiniLM     │
 └─────┬──────┘ └─────┬──────┘          └─────┬──────┘
       │               │                       │
       └───────┬───────┘                       │
               │ Kafka edges                   │
               ▼                               ▼
        ┌──────────────┐                ┌──────────────┐
        │ OS Sink      │                │ OS Sink      │
        │ (en index)   │                │ (intl index) │
        └──────────────┘                └──────────────┘
```

#### 4.3.7 DAG Pipeline Diagram (draw.io CSV)

```csv
# label: %name%<br><i style="font-size:9px">%desc%</i>
# style: shape=%shape%;fillColor=%fill%;strokeColor=%stroke%;fontColor=#333333;fontSize=11;html=1;whiteSpace=wrap;rounded=1;arcSize=10;
# namespace: df-dag-
# connect: {"from":"id", "to":"refs", "style":"curved=1;endArrow=block;endFill=1;strokeColor=%color%;fontSize=9;", "label":"%label%"}
# width: 170
# height: 60
# padding: 15
# ignore: id,fill,stroke,refs,shape,desc,label,color
# layout: horizontalflow
## ---
id,name,desc,shape,fill,stroke,color,refs,label
CONN,S3 Connector,CONNECTOR node,rectangle,#999999,#666666,#666666,INTAKE,Kafka edge
INTAKE,Intake Gate,Entry node (Tier 1 config),rectangle,#438DD5,#3C7FC0,#666666,PARSER,Kafka edge
PARSER,Parser (Tika),PROCESSOR node / L2 hydration,rectangle,#085B1D,#064516,#666666,"CHUNK_A;CHUNK_B",gRPC edge (CEL fan-out)
CHUNK_A,Chunker A,Token-based (512 tokens),rectangle,#085B1D,#064516,#2E7D32,"EMBED_MINI;EMBED_BGE",lang=='en'
CHUNK_B,Chunker B,Sentence-based (NLP),rectangle,#085B1D,#064516,#1565C0,EMBED_MINI_B,lang!='en'
EMBED_MINI,Embedder: MiniLM,384d vectors,rectangle,#085B1D,#064516,#2E7D32,SINK_EN,gRPC
EMBED_BGE,Embedder: BGE,1024d vectors,rectangle,#085B1D,#064516,#2E7D32,SINK_EN,gRPC
EMBED_MINI_B,Embedder: MiniLM,384d vectors,rectangle,#085B1D,#064516,#1565C0,SINK_INTL,gRPC
SINK_EN,OpenSearch Sink,English index,rectangle,#085B1D,#064516,#2E7D32,OS,Index
SINK_INTL,OpenSearch Sink,International index,rectangle,#085B1D,#064516,#1565C0,OS,Index
OS,AWS OpenSearch,"Hybrid text + k-NN search",cylinder,#FF8C00,#CC7000,#666666,,
DLQ,Global DLQ,Failed documents (any node),cylinder,#CC0000,#990000,#666666,,
```

### 4.4 Frontend Application

#### 4.4.1 Overview

The platform frontend is a **Vue 3 + Vuetify 3** single-page application backed by a **Node.js (Express)** backend-for-frontend (BFF). It communicates with all platform services via **Connect-ES** — a modern gRPC-web protocol that uses binary Protobuf over HTTP, providing type-safe RPC from the browser.

The frontend is a **pnpm monorepo** structured as:

```
pipestream-frontend/
├── apps/pipestream-frontend/
│   ├── src/                          # Express BFF (Node.js)
│   │   ├── index.ts                  # Server + system endpoints
│   │   ├── lib/serviceResolver.ts    # Dynamic service resolution
│   │   └── routes/connectRoutes.ts   # Connect-ES RPC proxy
│   └── ui/                           # Vue 3 SPA
│       └── src/
│           ├── router/               # Vue Router (lazy-loaded)
│           ├── stores/               # Pinia state management
│           ├── services/             # Embedded service UIs
│           └── pipeline-modules/     # Module configuration UIs
├── packages/
│   ├── protobuf-forms/               # Generated Protobuf stubs + form builders
│   ├── shared-nav/                   # Navigation shell components
│   ├── shared-components/            # Reusable Vuetify components
│   └── connector-shared/             # Connector utilities
└── pnpm-workspace.yaml
```

#### 4.4.2 Service Discovery-Driven UI

The frontend does not hardcode service locations. On startup, the Express BFF connects to the **Platform Registration Service** via a `WatchServices` streaming RPC and maintains a live registry of all available services — their names, hosts, ports, health status, and capabilities.

The Vue SPA polls this registry every 5 seconds via its Pinia store, and the navigation menu is **built dynamically** from discovered services. Services are categorized into:

| Category | Examples |
|----------|----------|
| **Core Platform** | Home, Health Dashboard, Components, Modules |
| **Services** | Account Manager, Connectors, Mapping, OpenSearch, Repository |
| **Pipeline Modules** | Chunker, Parser, Embedder, Echo |
| **Infrastructure** | Consul |

If a service is unavailable, its menu entry is hidden or shown as degraded. If the Platform Registration Service itself is unreachable, the UI falls back to a system error page.

#### 4.4.3 Connect-ES Communication

All frontend-to-backend communication uses the Connect-ES protocol (binary Protobuf by default, with JSON fallback for debugging):

```
Browser (Vue 3)
    │
    │  Connect-ES RPC (binary Protobuf over HTTP POST)
    │  e.g., POST /ai.pipestream.design_mode.DesignModeService/CreateDesignGraph
    │
    ▼
Express BFF (port 38106)
    │
    │  Dynamic transport: resolves service host:port from live registry
    │  createDynamicTransport(serviceName)
    │
    ▼
Platform Service (gRPC)
    e.g., pipestream-engine:38101
```

The BFF proxies requests to the correct backend service by resolving its location from the service registry. This means the frontend never needs to know where services are deployed — it only knows the BFF endpoint.

#### 4.4.4 Key Frontend Capabilities

**Pipeline Designer (DAG Editor):**
- Create, list, validate, and deploy pipeline graphs
- Visual canvas with node placement (`DesignModeConfig.canvas_x/y`)
- Module configuration via JSON Forms generated from Apicurio-registered JSON schemas
- Pipeline simulation with configurable success rates and latency
- Graph validation (acyclicity, module existence, CEL syntax)
- One-click deployment from DESIGN to PRODUCTION mode

**Service Management UIs:**
- **Account Manager** — account CRUD, activation/deactivation
- **Connector Admin** — datasource configuration, API key management, connector type browsing
- **Repository Browser** — document search, import/export, metadata inspection
- **OpenSearch Manager** — index and mapping management
- **Mapping Editor** — field transformation configuration

**Module Configuration:**
- Per-module configuration pages at `/modules/{name}`
- Configuration schemas fetched from modules at registration time
- JSON Forms rendering for type-safe configuration editing
- Health monitoring per module instance

**System Diagnostics:**
- `/health` — aggregated health status across all services
- Service connectivity verification
- Cache invalidation endpoint for service registry refresh

#### 4.4.5 Deployment

| Environment | Configuration |
|-------------|---------------|
| **Development** | Vite dev server (port 33000) → proxy to Express BFF (port 38106) → services |
| **Production** | Express serves built Vue assets from `/public`; single container on Fargate |
| **Port** | 38106 (BFF + static assets) |
| **Service discovery** | `PLATFORM_REGISTRATION_HOST` / `PLATFORM_REGISTRATION_PORT` environment variables |

#### 4.4.6 Frontend Architecture Diagram (draw.io CSV)

```csv
# label: %name%<br><i style="font-size:9px">%desc%</i>
# style: shape=%shape%;fillColor=%fill%;strokeColor=%stroke%;fontColor=#333333;fontSize=11;html=1;whiteSpace=wrap;rounded=1;arcSize=10;
# namespace: df-frontend-
# connect: {"from":"id", "to":"refs", "style":"curved=1;endArrow=block;endFill=1;strokeColor=#666666;fontSize=9;", "label":"%label%"}
# width: 180
# height: 65
# padding: 15
# ignore: id,fill,stroke,refs,shape,desc,label
# layout: horizontalflow
## ---
id,name,desc,shape,fill,stroke,refs,label
USER,Administrator / User,Browser client,ellipse,#999999,#666666,VUE,HTTPS
VUE,Vue 3 SPA,"Vuetify 3 + Pinia + Vue Router",rectangle,#438DD5,#3C7FC0,BFF,"Connect-ES (binary Protobuf)"
BFF,Express BFF,"Node.js backend-for-frontend [Fargate, port 38106]",rectangle,#438DD5,#3C7FC0,"RESOLVER;PROXY",Dynamic routing
RESOLVER,Service Resolver,"WatchServices stream from Platform Registration",rectangle,#438DD5,#3C7FC0,PLAT_REG,Live registry
PLAT_REG,Platform Registration,Service + module + schema registry,rectangle,#438DD5,#3C7FC0,"CONSUL;APICURIO",Discovery + schemas
CONSUL,Consul,Service health + endpoints,rectangle,#E8E8E8,#CCCCCC,,
APICURIO,Apicurio Registry,"Protobuf + JSON config schemas",rectangle,#438DD5,#3C7FC0,,
PROXY,Connect-ES Proxy,Routes RPC to resolved service,rectangle,#438DD5,#3C7FC0,"ENGINE;ACCT;CADMIN;REPO;OSMGR",gRPC
ENGINE,PipeStream Engine,Pipeline config + execution,rectangle,#438DD5,#3C7FC0,,
ACCT,Account Service,Account management,rectangle,#438DD5,#3C7FC0,,
CADMIN,Connector Admin,Datasource + API key mgmt,rectangle,#438DD5,#3C7FC0,,
REPO,Repository Service,Document storage,rectangle,#438DD5,#3C7FC0,,
OSMGR,OpenSearch Manager,Index management,rectangle,#438DD5,#3C7FC0,,
MATOMO,Matomo,Search usage analytics,rectangle,#E8E8E8,#CCCCCC,,
```

---

## 5. Architectural Pillars

### 5.1 Security and Compliance

#### 5.1.1 Zero-Trust Network Architecture

The platform implements a zero-trust security model where no service implicitly trusts another, regardless of network location.

**Service Mesh (mTLS via Consul Connect):**
All east-west (service-to-service) communication is encrypted and authenticated using mutual TLS managed by Consul Connect. Every Fargate task and EC2 instance runs a Consul sidecar proxy that:
- Terminates and originates mTLS connections using short-lived, auto-rotated certificates
- Enforces service identity — a request from `pipestream-engine` to `module-parser` is cryptographically verified at both ends
- Applies intention-based access control — only explicitly authorized service-to-service paths are permitted (e.g., engine → parser: allow; parser → engine: deny)

**External TLS Termination:**
External-facing traffic (connectors, admin UI, search UI) terminates TLS at the AWS Application Load Balancer. GPU-backed services (DJL Serving, Docling Serve) use ALB-level SSL termination with traffic forwarded over the Consul mesh internally.

**No Implicit Trust Zones:**
VPC security groups provide defense-in-depth but are not relied upon as a primary access control. Even within the same subnet, services must authenticate via mTLS.

#### 5.1.2 Encryption

**Data at Rest:**
| Layer | Mechanism |
|-------|-----------|
| S3 document blobs | SSE-KMS (AWS-managed CMK with key rotation) |
| Aurora PostgreSQL | AES-256 encryption at rest (RDS-managed) |
| OpenSearch indices | Encryption at rest (AWS-managed) |
| Kafka messages (MSK) | SSE with KMS-managed keys |
| Secrets | AWS Secrets Manager (encrypted with KMS) |

**Data in Transit:**
| Path | Mechanism |
|------|-----------|
| Service-to-service (east-west) | mTLS via Consul Connect (TLS 1.3) |
| External-to-ALB (north-south) | TLS 1.2+ at ALB |
| GPU services (DJL, Docling) | TLS terminated at ALB, mTLS internally |
| Kafka client-to-broker | TLS 1.2+ (MSK in-transit encryption) |
| Aurora client connections | SSL/TLS enforced |

#### 5.1.3 Credential and Secret Management

- All credentials (database passwords, API keys, external service tokens) are stored in **AWS Secrets Manager**, encrypted with KMS
- Datasource API keys are hashed using **Argon2id** before storage — raw keys are never persisted
- API key rotation is supported without downtime via the `RotateApiKey` gRPC endpoint
- No credentials are stored in environment variables, configuration files, or source code
- Secrets Manager automatic rotation is enabled for database credentials

#### 5.1.4 IAM and Service Identity

**IAM Execution Roles:**
A shared IAM execution role is used for Fargate tasks with policies scoped to the minimum required permissions:

| Permission | Scope |
|------------|-------|
| S3 read/write | Platform document buckets only |
| KMS encrypt/decrypt | Platform CMK only |
| Secrets Manager read | Platform secrets only |
| MSK produce/consume | Platform topics only |
| OpenSearch index/query | Platform domains only |
| CloudWatch logs/metrics | Platform log groups only |

As the platform matures, the shared role will be decomposed into per-service roles following the principle of least privilege.

#### 5.1.5 User and Connector Identity

**Connector Authentication (Okta-Backed):**
The connector-to-intake boundary is the external trust boundary. Connectors authenticate to the Intake Gate using:
1. **API key validation** — each datasource is issued a unique API key, validated via Argon2id hash comparison
2. **Okta token validation** — the Intake Gate validates Okta-issued tokens for connector identity, ensuring that only authorized connectors can push data for a given account and datasource

**OpenSearch Dashboard Access:**
Administrative access to OpenSearch Dashboards uses Okta SAML integration for SSO, with role-based access control (RBAC) mapped from Okta groups.

#### 5.1.6 Document-Level Access Control

ACLs from source systems are preserved as first-class metadata throughout the pipeline:

1. **Intake** — the Ownership Enrichment Service resolves ACLs from external governance systems and attaches them to the document's `OwnershipContext`
2. **Processing** — ACL metadata is carried through every DAG node without modification
3. **Indexing** — ACLs are stored as structured fields in OpenSearch documents
4. **Query** — a Quarkus-based Search Proxy Service intercepts all queries, resolves the caller's identity, and injects ACL filter clauses into the OpenSearch query DSL before execution

This ensures that even with a shared index, users see only the documents they are authorized to access.

---

### 5.2 Resilience and Reliability

#### 5.2.1 Compute Resilience

| Component | Deployment | HA Strategy |
|-----------|-----------|-------------|
| Core services (Intake, Engine, Repository, etc.) | AWS Fargate | Multi-AZ task placement; ECS service auto-recovery; minimum healthy percent: 100% |
| Kafka Sidecar | AWS Fargate | Deployed alongside engine; Kafka consumer group rebalancing on failure |
| GPU modules (DJL, Docling) | EC2 with ASG | Auto Scaling Groups across AZs; health check-based replacement; ALB distributes traffic |
| Consul cluster | EC2 with ASG | 3-node cluster across 3 AZs; Raft consensus tolerates 1 node failure; ASG replaces unhealthy nodes |

#### 5.2.2 Data Resilience

| System | RPO | RTO | Mechanism |
|--------|-----|-----|-----------|
| Aurora PostgreSQL | ~5 min | ~30 sec | Continuous backup + automatic multi-AZ failover; point-in-time recovery up to 35 days |
| AWS S3 | 0 (durable) | 0 (always available) | 11 nines durability; versioning enabled; cross-region replication available |
| AWS MSK | ~0 | ~minutes | Replication factor 2 across AZs; automatic broker recovery; 30-day retention |
| OpenSearch | Rebuildable | ~2 hours | Indices are derived data — full rebuild from Kafka replay or S3 document re-ingestion |

#### 5.2.3 Dead Letter Queue Strategy

The engine implements a multi-tier failure handling strategy:

1. **Retry** — module calls retry up to 3 times with exponential backoff (100ms initial, 2000ms max) for transient failures (UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED)
2. **DLQ** — after retry exhaustion, the document is published to a per-node DLQ topic (`pipestream.{cluster}.{node_id}.dlq`)
3. **Quarantine** — documents that fail DLQ processing are moved to a quarantine topic for manual investigation
4. **Global DLQ** — a fallback topic (`pipestream.global.dlq`) catches any failures that escape per-node DLQ routing

DLQ topics are monitored via Prometheus counters and Grafana alerts.

#### 5.2.4 Index Rebuild Strategy

OpenSearch indices are treated as derived, rebuildable state:
- **30-day Kafka retention** allows full replay of recent documents
- **Scheduled full-recall** reprocessing rebuilds clean indices from S3-stored documents on a configurable interval
- **Blue-green indexing** — new indices are built alongside old ones; alias swap provides zero-downtime cutover

#### 5.2.5 Recovery Targets

| Scenario | RPO | RTO |
|----------|-----|-----|
| Single Fargate task failure | 0 (Kafka offsets) | < 1 min (ECS auto-recovery) |
| Aurora AZ failure | ~5 min | ~30 sec (automatic failover) |
| MSK broker failure | 0 (replication) | ~5 min (automatic recovery) |
| OpenSearch cluster degradation | Rebuildable | ~2 hours (index rebuild) |
| Full region failure | ~1 hour (cross-region S3) | ~4 hours (infrastructure rebuild) |
| Consul node failure | 0 (Raft consensus) | ~2 min (ASG replacement) |

---

### 5.3 Performance and Efficiency

#### 5.3.1 Throughput Targets

| Metric | Target | Strategy |
|--------|--------|----------|
| Document ingestion | 1,000,000 docs/hour | Horizontally scaled Fargate intake tasks; Kafka partitioning for parallelism |
| End-to-end processing latency | p95 < 15 min | gRPC fast-path for in-memory hops; Kafka for durable boundary crossings only |
| Parser throughput | Scales with GPU fleet | EC2 ASG scales DJL and Docling instances based on queue depth |
| Embedding throughput | Scales with GPU fleet | DJL Serving batches inference requests; ASG scales on GPU utilization |
| OpenSearch indexing | Near real-time | Bulk indexing via OpenSearch Manager; index refresh interval tuned to workload |

#### 5.3.2 Transport Optimization

The engine supports two transport modes per graph edge, chosen at pipeline design time:

**gRPC (Low Latency):**
- Direct service-to-service calls within the mesh
- Used for tightly coupled processing chains (parser → chunker → embedder)
- Sub-millisecond handoff overhead
- No persistence between hops — suited for RTF workloads

**Kafka (Durable):**
- Used for cross-cluster routing, horizontal fan-out, and durable handoffs
- Documents are dehydrated to a `DocumentReference` and persisted to S3 before Kafka handoff
- Enables independent scaling of producer and consumer services
- 30-day retention provides replay capability

#### 5.3.3 Hydration Efficiency

The claim-check pattern minimizes data transfer:
- **Level 1 (metadata):** the `.pb` file (~KB) containing document metadata, parsed metadata, and `Any` payload — fetched on every node
- **Level 2 (content):** the `.bin` file (potentially GB) containing raw binary — fetched only when the module requires blob access (e.g., parser nodes)
- Hydration is lazy and node-aware — the engine inspects module capabilities before deciding whether to fetch L2 content

#### 5.3.4 Combinatorial Processing Efficiency

Fan-out processing (multiple chunkers × multiple embedding models) is optimized:
- Parsing happens once per document; parsed output is shared across all downstream paths
- Each chunker-embedder combination runs as an independent DAG branch
- Branches execute concurrently across separate Fargate tasks
- Results converge at sink nodes where each semantic result set is indexed independently

#### 5.3.5 GPU Auto-Scaling

| Service | Instance | Scaling Metric | Policy |
|---------|----------|---------------|--------|
| DJL Serving | EC2 GPU (ASG) | Kafka consumer lag + GPU utilization | Scale out at 70% GPU utilization or 10k+ consumer lag; scale in after 10 min idle |
| Docling Serve | EC2 GPU (ASG) | Kafka consumer lag + GPU utilization | Same policy as DJL |

---

### 5.4 Operational Excellence

#### 5.4.1 Observability Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| Metrics | Prometheus + Grafana | Service-level metrics (throughput, latency, error rates, DLQ depth, Kafka lag) |
| Tracing | OpenTelemetry + Dynatrace | Distributed trace propagation across all gRPC and Kafka hops; full document journey visibility |
| Logging | CloudWatch Logs | Structured JSON logs with MDC correlation (trace_id, stream_id, node_id, doc_id) |
| APM | Dynatrace | Application performance monitoring, anomaly detection, service flow mapping |
| Search Analytics | Matomo | Query patterns, click-through rates, result relevance tracking |

**Key Dashboards:**
- Pipeline throughput (docs/sec per node, per graph, per account)
- Module latency histograms (p50, p95, p99 per module type)
- DLQ depth and quarantine growth rate
- Kafka consumer lag per topic and consumer group
- GPU utilization and scaling events
- OpenSearch indexing rate and query latency
- S3 storage growth and cost projection

#### 5.4.2 Schema Governance

Apicurio Registry v3 serves as the single source of truth for all schemas:

| Schema Type | Purpose | Lifecycle |
|-------------|---------|-----------|
| Protobuf message schemas | Kafka serialization/deserialization validation | Published from `pipestream-protos`; breaking change detection via CI |
| gRPC service definitions | API contract registry for service discovery | Registered at build time; versioned alongside service releases |
| Module JSON config schemas | DAG editor configuration UI (JSON Forms) | Registered at module startup; drives visual configuration in the admin UI |

#### 5.4.3 Service Discovery and Health

Consul provides runtime service discovery with health monitoring:
- Every service registers on startup with gRPC health check endpoints
- SmallRye Stork provides client-side load balancing using Consul as the service locator
- Unhealthy instances are automatically deregistered after 3 consecutive failed health checks
- The Platform Registration Service provides a unified view of all registered services, modules, and their capabilities

#### 5.4.4 Pipeline Lifecycle Management

The `PipelineConfigService` gRPC API supports full pipeline lifecycle:

| Operation | Description |
|-----------|-------------|
| Design mode | Pipeline graphs are created and edited with simulated processing (configurable success rates, latency) |
| Validation | Graph structure is validated (DAG acyclicity, node module existence, CEL expression syntax) |
| Activation | Graphs move from DESIGN to PRODUCTION mode with versioned snapshots |
| Live updates | `WatchPipelineGraph` streaming RPC pushes graph changes to engine instances in real time |
| Rollback | Previous graph versions are retained in PostgreSQL JSONB; rollback is a version pointer change |

#### 5.4.5 Deployment Strategy

- All services are containerized (UBI9 base images) and deployed via Fargate
- Blue-green deployment for zero-downtime releases
- Quarkus native compilation evaluated for cold-start optimization
- Infrastructure as Code (CloudFormation / CDK) for reproducible environments
- Separate environments: dev, staging, production — each with isolated VPCs and data stores

---

### 5.5 Cost Optimization

#### 5.5.1 Compute Cost Management

| Strategy | Implementation |
|----------|---------------|
| Right-sizing | Fargate tasks sized to actual workload (CPU/memory tuned per service profile) |
| GPU efficiency | ASG scales GPU instances to zero during off-peak hours; spot instances for non-critical batch processing |
| Serverless where appropriate | Fargate eliminates idle EC2 cost for non-GPU services |
| Consul cluster | 3-node EC2 cluster (not oversized to 5-node) — sufficient for current scale |

#### 5.5.2 Storage Cost Management

| Strategy | Implementation |
|----------|---------------|
| S3 lifecycle policies | Standard → Infrequent Access (30 days) → Glacier (90 days) for archived documents |
| Blob dehydration | Binary payloads dropped after parsing when configured (`drop_blobs_after_parse`) — reduces S3 growth |
| OpenSearch tiering | Hot-warm architecture: recent indices on hot nodes, aged indices on warm (UltraWarm) storage |
| Aurora right-sizing | Serverless v2 evaluated for variable workloads; reserved instances for steady-state |
| Kafka retention | 30-day retention balances replay capability against storage cost |

#### 5.5.3 Data Transfer Cost Management

| Strategy | Implementation |
|----------|---------------|
| Claim-check pattern | Only document references (not payloads) traverse Kafka — reduces MSK data transfer |
| gRPC fast-path | In-memory processing avoids S3 round-trips for tightly coupled nodes |
| AZ-aware routing | Consul service discovery prefers same-AZ instances to minimize cross-AZ data transfer |
| RTF mode | Right-to-Forget processing avoids all S3 storage and retrieval costs |

#### 5.5.4 Projected Cost Drivers

| Component | Cost Driver | Optimization Lever |
|-----------|------------|-------------------|
| GPU instances (DJL, Docling) | Largest single cost | Batch processing; spot instances; model optimization; right-size instance types |
| AWS MSK | Broker hours + storage | 6-broker cluster sized for throughput; retention tuned to 30 days |
| Aurora PostgreSQL | Instance hours + storage | Per-service clusters right-sized independently; Serverless v2 for bursty workloads |
| S3 | Storage + requests | Lifecycle policies; blob dehydration; efficient hydration patterns |
| OpenSearch | Instance hours + storage | Hot-warm tiering; index lifecycle management; shard count tuned to data volume |
| Data transfer | Cross-AZ + internet egress | Same-AZ preference; claim-check pattern; gRPC fast-path |
