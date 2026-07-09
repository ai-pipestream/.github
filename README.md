# Pipestream AI

**Open-source software that assists with human understanding.**

[![License: Apache 2.0 / MIT](https://img.shields.io/badge/license-Apache--2.0%20%2F%20MIT-blue.svg)](#-license)
[![Website](https://img.shields.io/badge/website-pipestream.ai-blue)](https://pipestream.ai)

Pipestream AI is a small collaboration of engineers working on distributed
semantic search, document understanding, and the open infrastructure that
connects them. We build in the open and contribute upstream — to Apache OpenNLP,
Tika, and Lucene, to Quarkus, and to the gRPC and Docling ecosystems.

The platform itself is a flexible, network-based architecture for ingesting,
parsing, chunking, and embedding documents for search and indexing. See our
[Public Research](docs/research/README.md) for the collaborative distributed
HNSW work.

## 📚 Documentation

### Getting Started

- **[Document Journey Guide](docs/design/document-journey.md)** - Comprehensive guide to how documents flow through the Pipestream Platform, from ingestion to indexing. Includes examples, network topologies, and technical details.

### Architecture & Design

- **[Documentation Index](docs/README.md)** - Overview of all platform documentation
- **[Document Journey](docs/design/document-journey.md)** - End-to-end document processing flow

### Platform Components

*Links to project-specific documentation will be added as features are documented:*

- **Pipeline Engine** - Core orchestration engine that routes documents through processing modules
- **Connectors** - Document ingestion services (S3, file system, API, etc.)
- **Repository Service** - Document storage in S3 and metadata management
- **Processing Modules**:
  - **Parser** - Extract text and metadata from various document formats (PDF, video, images, etc.)
  - **Chunker** - Split text into semantic chunks using multiple strategies
  - **Embedder** - Generate vector embeddings using multiple models
  - **Sink** - Index documents to OpenSearch with full-text, vector, and hybrid search

### Development & Setup

- **[Site Setup Guide](SETUP.md)** - Instructions for deploying the homepage via GitHub Pages
- **[Troubleshooting](TROUBLESHOOTING.md)** - Common issues and solutions

## 🏗️ Architecture Overview

The Pipestream Platform operates as a **network graph**, not a linear pipeline. The Pipeline Engine acts as the central routing hub, orchestrating data flow between processing nodes:

1. **Data Loading** - Digital assets are ingested through connectors, direct API calls, or Kafka events
2. **Data Transformation** - Assets are transformed to text using parsers (Apache Tika, specialized parsers)
3. **Data Enhancement** - Text is enhanced with chunking, embeddings, and AI processing
4. **Sink** - Data is indexed to OpenSearch with full-text, vector, and hybrid search capabilities

### Key Features

- **Network Graph Architecture** - Fan-in and fan-out capabilities for flexible routing
- **Multiple Entry Points** - Connectors, direct API, or Kafka events
- **Flexible Storage** - S3 repository or in-memory processing
- **Multiple Chunking Strategies** - Apply different chunking approaches to accumulate chunks
- **Multiple Embedding Models** - Generate vector embeddings using multiple models simultaneously
- **Transport Flexibility** - gRPC for low latency, Kafka for high throughput
- **Dynamic Routing** - Runtime routing decisions based on configuration

## 🤝 Contributing

Pipestream AI is open source under the MIT License. We welcome contributions!

1. Check out our [documentation](docs/)
2. Review the [architecture guide](docs/design/document-journey.md)
3. Open issues or pull requests in the relevant repositories

## 📦 Repositories

This organization contains multiple repositories for different components of the platform. See the [organization profile](profile/README.md) for more information.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file in each repository for details.

---

## 🌐 Live Site

The Pipestream AI homepage is deployed via GitHub Pages:

- **Primary Domain**: https://pipestream.ai
- **GitHub Pages URL**: https://ai-pipestream.github.io/ai-pipestream-homepage/

### Site Setup & Development

For information about setting up and deploying the homepage:

- **[SETUP.md](SETUP.md)** - Complete setup instructions including DNS configuration and SSL setup
- **[TROUBLESHOOTING.md](TROUBLESHOOTING.md)** - Common issues and solutions for GitHub Pages deployment

### Quick Development

Preview changes locally:
```bash
# Simple Python server
python3 -m http.server 8000

# Or with Node.js
npx http-server
```

Then visit http://localhost:8000

---

**Building the future of intelligent document processing.** 🚀
