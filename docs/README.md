# Pipestream AI Documentation

Welcome to the Pipestream AI documentation. This directory contains general documentation about the platform architecture, design, and concepts.

Keep in mind, any documentation under "hamster-wheel" will be accompanied by a slew of AI Slop (AIS).  It's conceptual thoughts put on documentation meant to be read as reference for ideas on what to do next.  A lot of exciting research will exist here - from dynamic growing kafka partitioning strategies, service discovery patterns, etc... 

A large part of this project is to solve common problems of developing pipelines.  The fact is, a lot of it is not a technology problem - it's a workflow problem.  We're designing a text factory that's made to make human made digital assets be consumed by machines and humans alike. As such, a lot of fresh ideas come up, and we can put those in github issues or if it's more research oriented, we can put it in the hamster wheel.

## 📚 Documentation Structure

### Design & Architecture
- **[Document Journey](design/document-journey.md)** - Comprehensive guide to how documents flow through the Pipestream Platform, from ingestion to indexing. Includes examples, network topologies, and technical details.

## 🔗 Related Documentation

For project-specific documentation, see the README files in each repository:
- [Pipeline Engine](../pipeline-engine/README.md) - Core orchestration engine
- [Connector Services](../connector-intake-service/README.md) - Document ingestion services
- [Repository Service](../repository-service/README.md) - Document storage and metadata management
- [Processing Modules](../modules/README.md) - Parser, chunker, embedder, and other processing modules

## 🤝 Contributing

Documentation improvements are welcome! Please open a pull request with your changes.

