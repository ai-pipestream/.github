# gRPC Performance Optimization Documentation

This directory contains documentation about gRPC performance optimizations implemented in the ai-pipestream platform.

## Documents

### [GRPC_PERFORMANCE_OPTIMIZATION.md](#/grpc/optimization/GRPC_PERFORMANCE_OPTIMIZATION)

Comprehensive guide on gRPC flow control window optimization:
- Problem statement and solution overview
- Technical details on HTTP/2 flow control
- Implementation details for client and server-side configuration
- Configuration examples
- Performance benchmarks
- Troubleshooting guide

### [GRPC_OPTIMIZATION_SUMMARY.md](#/grpc/optimization/GRPC_OPTIMIZATION_SUMMARY)

High-level summary of the system-wide implementation:
- What was done
- Performance results
- Configuration requirements
- Services status
- Next steps

### [UNIFIED_SERVER_LIMITATIONS.md](#/grpc/optimization/UNIFIED_SERVER_LIMITATIONS)

Analysis of unified vs separate server mode limitations:
- Why unified server mode doesn't support flow control window configuration
- Investigation results
- Performance impact
- Recommendation to use separate server mode

## Quick Reference

### Configuration

```properties
# Enable separate server mode (REQUIRED)
quarkus.grpc.server.use-separate-server=true

# Flow control window (100MB default)
quarkus.grpc.server.flow-control-window=104857600
quarkus.grpc.clients."*".flow-control-window=104857600

# Message size limits
quarkus.grpc.server.max-inbound-message-size=2147483647
quarkus.grpc.server.max-outbound-message-size=2147483647
quarkus.grpc.clients."*".max-inbound-message-size=2147483647
quarkus.grpc.clients."*".max-outbound-message-size=2147483647
```

### Performance Results

- **Before**: 5-10 MB/s for large messages
- **After**: 250-370 MB/s for large messages
- **Improvement**: 25-73x faster

## Related Documentation

- [Dynamic gRPC Library README](../../../platform-libraries/libraries/dynamic-grpc/README.md)
- [Dynamic gRPC Context](../../../platform-libraries/libraries/dynamic-grpc/CONTEXT.md)

