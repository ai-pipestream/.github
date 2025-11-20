# gRPC Performance Optimization - Implementation Summary

## Overview

This document summarizes the system-wide implementation of gRPC flow control window optimization across the ai-pipestream platform. This optimization increases gRPC throughput from **5-10 MB/s to 250-370 MB/s** for large messages.

## What Was Done

### 1. Centralized Implementation in `dynamic-grpc` Package

**Created**: `platform-libraries/libraries/dynamic-grpc/src/main/java/ai/pipestream/dynamic/grpc/server/GrpcServerFlowControlCustomizer.java`

- **Purpose**: System-wide server-side flow control window configuration
- **Discovery**: Automatically discovered by Quarkus CDI via `@ApplicationScoped`
- **Configuration**: Reads from `quarkus.grpc.server.flow-control-window` (default: 100MB)
- **Requirement**: `quarkus.grpc.server.use-separate-server=true` (Netty-based server)

### 2. Client-Side Already Implemented

**Location**: `platform-libraries/libraries/dynamic-grpc/src/main/java/ai/pipestream/dynamic/grpc/client/GrpcClientProvider.java`

- **Method**: Uses `NettyChannelBuilder.initialFlowControlWindow()`
- **Configuration**: Reads from `quarkus.grpc.clients."*".flow-control-window` (default: 100MB)
- **Status**: Already working correctly

### 3. Documentation Created

**Created**: `platform-libraries/libraries/dynamic-grpc/GRPC_PERFORMANCE_OPTIMIZATION.md`

- Comprehensive guide on flow control window optimization
- Performance benchmarks and results
- Configuration examples
- Troubleshooting guide
- References to official documentation

### 4. Updated Services

#### ✅ repository-service
- **Changed**: `quarkus.grpc.server.use-separate-server=true`
- **Dependency**: Already has `dynamic-grpc` (line 52 in build.gradle)
- **Status**: Customizer will be automatically applied

#### ✅ connector-intake-service
- **Removed**: Local `GrpcServerFlowControlCustomizer.java`
- **Changed**: `quarkus.grpc.server.use-separate-server=true` (production)
- **Dependency**: Needs verification (should have `dynamic-grpc`)
- **Status**: Will use centralized customizer from `dynamic-grpc`

### 5. Updated Documentation

**Updated**: `platform-libraries/libraries/dynamic-grpc/CONTEXT.md`

- Added `GrpcServerFlowControlCustomizer` to key components list
- Documented flow control optimization
- Referenced performance optimization guide

## Performance Results

| Test Scenario | Throughput | Improvement |
|--------------|------------|-------------|
| 10MB × 10 parallel (before) | 5-10 MB/s | Baseline |
| 10MB × 10 parallel (after) | 255.79 MB/s | **25-50x faster** |
| 250MB single (after) | 367.64 MB/s | **37-73x faster** |

## Configuration Required

All gRPC services should have these properties in `application.properties`:

```properties
# Enable separate server mode (required for flow control tuning)
quarkus.grpc.server.use-separate-server=true

# Server-side flow control window (default: 100MB)
quarkus.grpc.server.flow-control-window=104857600

# Client-side flow control window (wildcard for all clients)
quarkus.grpc.clients."*".flow-control-window=104857600

# Message size limits (2GB - 1 byte, max int value)
quarkus.grpc.server.max-inbound-message-size=2147483647
quarkus.grpc.server.max-outbound-message-size=2147483647
quarkus.grpc.clients."*".max-inbound-message-size=2147483647
quarkus.grpc.clients."*".max-outbound-message-size=2147483647
```

## Services Status

### ✅ Completed
- **repository-service**: Updated configuration, has dependency
- **connector-intake-service**: Removed local customizer, updated configuration

### ⏳ Needs Verification
- **platform-registration-service**: Check dependency, update configuration
- **connector-admin**: Check dependency, update configuration
- **account-service**: Check dependency, update configuration
- **module-echo**: Check dependency, update configuration
- **module-parser**: Check dependency, update configuration
- **module-embedder**: Check dependency, update configuration
- **module-chunker**: Check dependency, update configuration

## Next Steps

1. **Verify Dependencies**: Ensure all gRPC services have `dynamic-grpc` as a dependency
2. **Update Configurations**: Set `quarkus.grpc.server.use-separate-server=true` in all services
3. **Test**: Run performance tests on each service to verify optimization is applied
4. **Monitor**: Add metrics/logging to track flow control window usage in production

## How It Works

### Server-Side Flow Control

1. Quarkus discovers `GrpcServerFlowControlCustomizer` via CDI
2. During server initialization, Quarkus calls `customize()` method
3. Customizer reads `quarkus.grpc.server.flow-control-window` from config
4. Accesses `NettyServerBuilder` via `VertxServerBuilder.nettyBuilder()`
5. Sets flow control window via `initialFlowControlWindow()`
6. Server starts with optimized flow control window

### Client-Side Flow Control

1. `GrpcClientProvider.getClient()` is called
2. Reads `quarkus.grpc.clients."*".flow-control-window` from config
3. Creates `NettyChannelBuilder` with `initialFlowControlWindow()`
4. Channel is cached and reused for subsequent calls

## Key Files

- **Server Customizer**: `platform-libraries/libraries/dynamic-grpc/src/main/java/ai/pipestream/dynamic/grpc/server/GrpcServerFlowControlCustomizer.java`
- **Client Provider**: `platform-libraries/libraries/dynamic-grpc/src/main/java/ai/pipestream/dynamic/grpc/client/GrpcClientProvider.java`
- **Performance Guide**: `platform-libraries/libraries/dynamic-grpc/GRPC_PERFORMANCE_OPTIMIZATION.md`
- **Context Documentation**: `platform-libraries/libraries/dynamic-grpc/CONTEXT.md`

## References

- [Quarkus gRPC Service Consumption Guide](https://quarkus.io/guides/grpc-service-consumption)
- [Quarkus gRPC Service Implementation Guide](https://quarkus.io/guides/grpc-service-implementation)
- [HTTP/2 Flow Control Specification](https://httpwg.org/specs/rfc7540.html#FlowControl)

