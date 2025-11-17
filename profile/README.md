# Pipestream AI

**Open Source Document Processing Platform for Intelligent Search and Indexing**

[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Website](https://img.shields.io/badge/website-pipestream.ai-blue)](https://pipestream.ai)

## 🌟 What is Pipestream AI?

Pipestream AI is an open-source platform that transforms documents into searchable knowledge using AI-powered processing. It provides a flexible, network-based architecture for ingesting, parsing, chunking, and embedding documents for intelligent search and indexing.

## 🚀 Key Features

- **Network Graph Architecture** - Not a linear pipeline, but a flexible network with fan-in and fan-out capabilities
- **Multiple Entry Points** - Connectors, direct API calls, or Kafka events
- **Flexible Storage** - S3 repository or in-memory processing
- **Multiple Chunking Strategies** - Apply different chunking approaches to the same document
- **Multiple Embedding Models** - Generate vector embeddings using multiple models simultaneously
- **OpenSearch Integration** - Full-text, vector, and hybrid search capabilities
- **Transport Flexibility** - gRPC for low latency, Kafka for high throughput

## 📖 Documentation

- **[Document Journey Guide](docs/design/document-journey.md)** - Comprehensive guide to how documents flow through the platform
- **[Architecture Overview](docs/)** - Platform architecture and design documentation
- **[Website](https://pipestream.ai)** - Visit our homepage for more information

## 🏗️ Architecture

The Pipestream Platform operates as a network graph, not a linear pipeline. The Pipeline Engine acts as the central routing hub, orchestrating data flow between processing nodes:

1. **Data Loading** - Digital assets are ingested
2. **Data Transformation** - Assets are transformed to text (parsing)
3. **Data Enhancement** - Text is enhanced with chunking, embeddings, and AI processing
4. **Sink** - Data is indexed to a search engine (OpenSearch)

## 🛠️ Core Services

- **Connectors** - Discover, authenticate, and stream documents from various sources
- **Repository Service** - Manages S3 storage and metadata, publishes events
- **Pipeline Engine** - Orchestrates routing and transport between modules
- **Processing Modules** - Parsers, chunkers, embedders, and specialized processors

## 📦 Repositories

This organization contains multiple repositories:

- **Core Services** - Platform services and orchestration
- **Processing Modules** - Specialized document processors
- **Connectors** - Document ingestion from various sources
- **Frontend** - Web interface and management tools

## 🤝 Contributing

Pipestream AI is open source under the MIT License. We welcome contributions!

1. Check out our [documentation](docs/)
2. Review the [architecture](docs/design/document-journey.md)
3. Open issues or pull requests in the relevant repositories

## 📄 License

This project is licensed under the MIT License - see the LICENSE file in each repository for details.

## 🔗 Links

- **Website**: https://pipestream.ai
- **GitHub Organization**: https://github.com/ai-pipestream
- **Documentation**: [docs/](docs/)

---

**Building the future of intelligent document processing.** 🚀
