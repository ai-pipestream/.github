# AI-Pipestream Platform: System Overview

## Introduction
The AI-Pipestream platform is a powerful, modular, and high-performance system designed for AI-driven data processing pipelines. It leverages a microservices architecture to provide scalability, resilience, and dynamic extensibility, allowing for seamless integration and orchestration of various data processing steps. Our focus is on efficient, flexible data flow through a "document hop" model, enabling complex AI workflows for diverse applications.

## Core Architectural Principles
*   **Microservices-driven:** The platform is built as a collection of loosely coupled, independently deployable services, ensuring modularity, scalability, and fault isolation.
*   **Dynamic & Extensible:** New functionality, referred to as "Modules," can be added, updated, or removed from the pipeline at runtime without requiring extensive system redeployments.
*   **High Performance:** Optimized for efficient data processing and transfer, particularly for large payloads, by leveraging technologies like gRPC with tuned flow control.
*   **Polyglot Capable:** While core services are often built with Java/Quarkus, the platform is designed to support Modules implemented in any language (e.g., Python, Rust) that can interface via gRPC.

## The AI-Pipestream Data Flow: "The Document Hop"
At the heart of AI-Pipestream is the "Document Hop" model, which describes the journey of a data payload, known as a **Pipedoc**, through a series of processing steps.
*   **Pipedoc:** A structured data object that encapsulates both the raw content and a rich set of metadata. As a Pipedoc traverses the pipeline, its metadata is continuously updated by each processing Module, providing a comprehensive audit trail and context for subsequent steps.
*   **Engine's Role:** The Engine (the central orchestration layer) manages this "hop." It receives a Pipedoc, determines the next Module in the pipeline based on configuration and metadata, dispatches the Pipedoc to that Module, and then receives it back for the next hop. This ensures a controlled and trackable flow of data.

## Key Architectural Components

### 1. The Engine (Orchestration Layer)
*(Note: The Engine is currently undergoing a complete rewrite to solidify its orchestration capabilities and performance.)*
*   **Responsibility:** The conceptual core of the pipeline, responsible for orchestrating Pipedoc processing, managing workflow state, and coordinating interactions between various Modules and Services.
*   **Interaction Mechanisms:**
    *   **gRPC:** Utilized for high-performance, synchronous communication with Modules, especially when low-latency processing and direct payload transfer are required.
    *   **Kafka:** Serves as a robust backbone for asynchronous, event-driven pipeline steps, enabling reliable message queuing, state management, and resilience against transient failures.
*   **Data Handling:** The Engine intelligently decides how to handle Pipedoc payloads. It can send heavy objects directly over gRPC (leveraging our flow control optimizations) or save them to the central [Repository Service](https://github.com/ai-pipestream/repository-service) or S3, passing only references for efficiency.

### 2. Platform Services (Core Microservices)
These foundational services provide essential capabilities and infrastructure for the entire platform.

*   **[Platform Registration Service](https://github.com/ai-pipestream/platform-registration-service)**: The central registry where all pipeline Modules and other dynamic services announce their presence and capabilities, making them discoverable by the Engine and other components.
*   **[Repository Service](https://github.com/ai-pipestream/repository-service)**: Provides core CRUD (Create, Read, Update, Delete) operations for Pipedocs and other Protobuf-based data. It acts as the canonical storage for all processed documents and their associated metadata.
*   **[Account Service](https://github.com/ai-pipestream/account-service)**: Manages user authentication, authorization, and multi-tenancy for the platform.
*   **[Connector Admin](https://github.com/ai-pipestream/connector-admin)**: An administrative interface for configuring, deploying, and managing various data connectors that feed external data into the AI-Pipestream platform.
*   **[Connector Intake Service](https://github.com/ai-pipestream/connector-intake-service)**: The primary entry point for external data. It ingests raw data, performs initial processing, and transforms it into Pipedocs for entry into the pipeline.
*   **[Mapping Service](https://github.com/ai-pipestream/mapping-service)**: Handles complex data transformations, schema validations, and data model mappings, ensuring compatibility between diverse data sources and processing Modules.
*   **[OpenSearch Manager](https://github.com/ai-pipestream/opensearch-manager)**: Provides dedicated services for interacting with OpenSearch clusters, including index management, data ingestion into OpenSearch, and search query execution.
*   **[Platform Libraries](https://github.com/ai-pipestream/platform-libraries)**: A collection of shared code, utility functions, and generated gRPC stubs (for both Java and NPM) that are consumed by almost all services and modules in the platform.
*   **Filesystem Crawler Service**: *(Currently under active development and architectural refinement. It will be responsible for ingesting data directly from various filesystem sources.)*

### 3. Pipeline Modules
Independent, domain-specific microservices designed to perform discrete processing steps on Pipedocs. They are the workhorses of the AI pipeline.

*   **Purpose:** Each Module focuses on a specific task, such as data enrichment, transformation, analysis, or integration.
*   **Examples (referencing their respective repositories):**
    *   **[Module Chunker](https://github.com/ai-pipestream/module-chunker)**: Breaks down large documents into smaller, more manageable chunks for granular processing.
    *   **[Module Echo](https://github.com/ai-pipestream/module-echo)**: A simple example module for testing and demonstration, often used to reflect Pipedocs or perform basic logging.
    *   **[Module Embedder](https://github.com/ai-pipestream/module-embedder)**: Generates vector embeddings from Pipedoc content, enabling semantic search and similarity-based AI operations.
    *   **[Module OpenSearch Sink](https://github.com/ai-pipestream/module-opensearch-sink)**: Responsible for indexing processed Pipedocs or their extracted features directly into OpenSearch.
    *   **[Module Parser](https://github.com/ai-pipestream/module-parser)**: Extracts structured information and text from various document formats (e.g., PDFs, Word documents) into a standardized Pipedoc structure.
    *   **[Module Pipeline Probe](https://github.com/ai-pipestream/module-pipeline-probe)**: Designed for monitoring and debugging Pipedocs as they flow through the pipeline.
    *   **[Module Proxy](https://github.com/ai-pipestream/module-proxy)**: Acts as an intermediary, potentially routing Pipedocs or applying common transformations before forwarding.
*   **Technology:** Primarily gRPC services, enabling efficient and fast inter-module communication.

## Technology Stack Highlights
*   **gRPC:** Chosen for its high performance, language neutrality, and efficient serialization (Protobufs). Critical platform components leverage advanced gRPC flow control tuning to ensure maximal throughput for large data transfers.
*   **Kafka:** Provides a scalable, fault-tolerant backbone for asynchronous messaging, event streaming, and reliable inter-service communication within the pipeline.
*   **Quarkus:** The preferred Java framework for microservices development, offering blazing-fast startup times, low memory consumption, and a highly productive developer experience.
*   **Consul:** Employed for dynamic service discovery, configuration management, and robust health checking across the distributed microservices architecture.
*   **OpenSearch:** Integrated for powerful full-text search, analytics, and data aggregation capabilities on processed Pipedocs and metadata.

## High-Level Data Flow Diagram
```mermaid
graph TD
    subgraph Data Ingestion
        A[External Data Source] --> B[Connector Intake Service]
    end

    subgraph Pipeline Processing (The Engine orchestrates)
        B -- Pipedoc Init --> C{Module 1: Chunker}
        C -- Processed Pipedoc --> D{Module 2: Parser}
        D -- Processed Pipedoc --> E{Module 3: Embedder}
        E -- Final Pipedoc --> F[Repository Service (Pipedoc Storage)]
    end

    subgraph Search & Analytics
        F -- Index Request --> G[OpenSearch Manager]
        G -- Indexed Data --> H[OpenSearch Index]
    end

    subgraph User Interaction
        I[Platform Frontend] --> J[Platform Services]
        J -- Data Access --> F
        H -- Search Results --> I
    end

    C -- gRPC / Kafka --> D
    D -- gRPC / Kafka --> E
    E -- gRPC / Kafka --> F
    B -- Registers Service --> K[Platform Registration Service]
    C -- Registers Service --> K
    D -- Registers Service --> K
    E -- Registers Service --> K
    F -- Registers Service --> K
    K -- Discovers Services --> B,C,D,E,F
```

## Future Vision
The AI-Pipestream platform is continuously evolving to enhance its capabilities in areas such as advanced AI model integration, real-time analytics, user-friendly pipeline configuration, and expanding its ecosystem of pluggable Modules. We aim to provide a robust and flexible foundation for innovation in data-driven AI applications.
