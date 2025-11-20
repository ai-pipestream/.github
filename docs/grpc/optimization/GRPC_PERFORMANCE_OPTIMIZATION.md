# gRPC Performance Optimization Guide

## Summary

This document describes the gRPC performance optimizations implemented across the ai-pipestream platform, specifically focusing on **flow control window tuning** to achieve high throughput for large messages (100MB+).

## Problem Statement

### Initial Performance Issue
- **Observed**: 5-10 MB/s throughput for large gRPC messages (10MB+)
- **Root Cause**: Default HTTP/2 flow control window of 64KB severely limits in-flight data
- **Impact**: Large document uploads taking 10-20 seconds instead of <1 second

### Solution
- **Implemented**: Increased flow control window to 100MB (default) or configurable
- **Result**: **367.64 MB/s** for 250MB single messages, **255.79 MB/s** for parallel 10MB messages
- **Improvement**: **50-70x performance increase**

## Technical Details

### Flow Control Window Explained

The HTTP/2 flow control window determines how much data can be sent before waiting for acknowledgment from the receiver. The default 64KB window means:

1. Sender sends 64KB
2. Waits for receiver to acknowledge
3. Receiver processes and sends WINDOW_UPDATE
4. Sender can send another 64KB
5. Repeat...

For a 250MB message, this results in ~4,000 round-trips, causing severe throughput bottlenecks.

### Our Solution

We configure both **client-side** and **server-side** flow control windows:

#### Client-Side (Already Implemented)
- **Location**: `GrpcClientProvider.java` in `dynamic-grpc` package
- **Method**: Uses `NettyChannelBuilder.initialFlowControlWindow()`
- **Configuration**: Reads from `quarkus.grpc.clients."*".flow-control-window`
- **Default**: 100MB (104857600 bytes)

#### Server-Side (New Implementation)
- **Location**: `GrpcServerFlowControlCustomizer.java` in `dynamic-grpc` package
- **Method**: Uses `ServerBuilderCustomizer` to access `NettyServerBuilder.initialFlowControlWindow()`
- **Configuration**: Reads from `quarkus.grpc.server.flow-control-window`
- **Default**: 100MB (104857600 bytes)
- **Requirement**: `quarkus.grpc.server.use-separate-server=true` (uses Netty-based server)
- **⚠️ Unified Server Mode**: When `use-separate-server=false`, the unified Vert.x HTTP server does not expose flow control window settings through `GrpcServerOptions`. This may be configurable via the main HTTP server's HTTP/2 settings (`quarkus.http.http2.*`), but this has not been tested. If you need flow control tuning, use `use-separate-server=true`.

## Implementation

### 1. Client-Side Configuration (`GrpcClientProvider`)

Already implemented in `platform-libraries/libraries/dynamic-grpc/src/main/java/ai/pipestream/dynamic/grpc/client/GrpcClientProvider.java`:

```java
private int getFlowControlWindow() {
    Config config = ConfigProvider.getConfig();
    return config.getOptionalValue("quarkus.grpc.clients.\"*\".flow-control-window", Integer.class)
            .orElse(104857600);  // 100MB default
}

// In getClient():
return NettyChannelBuilder
    .forAddress(host, port)
    .initialFlowControlWindow(flowControlWindow)  // HTTP/2 initial window size
    .maxInboundMessageSize(maxInboundMessageSize)
    .usePlaintext()
    .build();
```

### 2. Server-Side Configuration (`GrpcServerFlowControlCustomizer`)

New implementation in `platform-libraries/libraries/dynamic-grpc/src/main/java/ai/pipestream/dynamic/grpc/server/GrpcServerFlowControlCustomizer.java`:

```java
@ApplicationScoped
public class GrpcServerFlowControlCustomizer implements ServerBuilderCustomizer {
    
    @Override
    public void customize(GrpcServerConfiguration config, ServerBuilder builder) {
        if (builder instanceof VertxServerBuilder) {
            VertxServerBuilder vertxBuilder = (VertxServerBuilder) builder;
            int flowControlWindow = ConfigProvider.getConfig()
                    .getOptionalValue("quarkus.grpc.server.flow-control-window", Integer.class)
                    .orElse(104857600);  // 100MB default
            
            NettyServerBuilder nettyBuilder = vertxBuilder.nettyBuilder();
            nettyBuilder.initialFlowControlWindow(flowControlWindow);
            
            LOG.infof("Set gRPC server flow control window to %d bytes (%d MB)", 
                    flowControlWindow, flowControlWindow / (1024 * 1024));
        }
    }
}
```

## Configuration

### Application Properties

All services should configure:

```properties
# Server-side flow control (requires use-separate-server=true)
quarkus.grpc.server.use-separate-server=true
quarkus.grpc.server.flow-control-window=104857600  # 100MB

# Client-side flow control (wildcard for all clients)
quarkus.grpc.clients."*".flow-control-window=104857600  # 100MB

# Message size limits (2GB - 1 byte, max int value)
quarkus.grpc.server.max-inbound-message-size=2147483647
quarkus.grpc.server.max-outbound-message-size=2147483647
quarkus.grpc.clients."*".max-inbound-message-size=2147483647
quarkus.grpc.clients."*".max-outbound-message-size=2147483647
```

### For Very Large Messages (500MB+)

Consider increasing the flow control window proportionally:

```properties
# For 500MB+ messages, use 250MB window
quarkus.grpc.server.flow-control-window=262144000  # 250MB
quarkus.grpc.clients."*".flow-control-window=262144000  # 250MB
```

## Performance Benchmarks

### Test Results

| Message Size | Configuration | Throughput | Notes |
|-------------|--------------|------------|-------|
| 10MB × 10 parallel | 100MB window | 255.79 MB/s | Parallel uploads |
| 250MB single | 100MB window | 367.64 MB/s | Single large message |
| 10MB × 10 parallel | 64KB default | 5-10 MB/s | **Before optimization** |

### Key Insights

1. **Single large messages perform better** than parallel small messages (367 MB/s vs 255 MB/s)
   - Less coordination overhead
   - Better pipelining with large flow control window

2. **Flow control window is the bottleneck**, not frame size
   - HTTP/2 frames are handled automatically by Netty
   - Default 16KB frames are sufficient

3. **100MB window is optimal** for most use cases
   - Balances memory usage and performance
   - Can be increased for very large messages (500MB+)

## Application to Services

### Services Using gRPC

1. **repository-service** ✅ (needs customizer)
2. **connector-intake-service** ✅ (has customizer, needs to move to dynamic-grpc)
3. **platform-registration-service** (needs customizer)
4. **connector-admin** (needs customizer)
5. **account-service** (needs customizer)
6. **module-echo** (needs customizer)
7. **module-parser** (needs customizer)
8. **module-embedder** (needs customizer)
9. **module-chunker** (needs customizer)

### Implementation Steps

For each service:

1. **Add dependency** on `dynamic-grpc` (if not already present)
2. **Configure properties** in `application.properties`:
   - `quarkus.grpc.server.use-separate-server=true`
   - `quarkus.grpc.server.flow-control-window=104857600`
   - `quarkus.grpc.clients."*".flow-control-window=104857600`
3. **Verify customizer is discovered** (CDI auto-discovery via `@ApplicationScoped`)
4. **Test with large messages** to verify throughput

## Troubleshooting

### Customizer Not Applied

**Symptoms**: Low throughput (5-10 MB/s) despite configuration

**Check**:
1. `quarkus.grpc.server.use-separate-server=true` is set
2. Customizer is in classpath (via `dynamic-grpc` dependency)
3. Log message: "Set gRPC server flow control window to..." appears at startup

**Solution**: Ensure `GrpcServerFlowControlCustomizer` is in `dynamic-grpc` package and service depends on it.

### Unified Server Mode Warning

**Symptoms**: Warning log: "Flow control window setting cannot be applied in unified server mode"

**Solution**: Set `quarkus.grpc.server.use-separate-server=true` to use Netty-based server.

### Port Binding Issues in Tests

**Symptoms**: `Port already bound` errors in tests

**Solution**: Set `quarkus.grpc.server.test-port=0` for random port assignment in test mode.

## References

- [Quarkus gRPC Service Consumption Guide](https://quarkus.io/guides/grpc-service-consumption)
- [Quarkus gRPC Service Implementation Guide](https://quarkus.io/guides/grpc-service-implementation)
- [gRPC Java Flow Control Documentation](https://github.com/grpc/grpc-java/blob/master/core/src/main/java/io/grpc/ManagedChannelBuilder.java)
- [HTTP/2 Flow Control Specification](https://httpwg.org/specs/rfc7540.html#FlowControl)

## Future Enhancements

1. **Dynamic Window Sizing**: Adjust flow control window based on message size
2. **Metrics**: Track flow control window usage and throughput
3. **Auto-tuning**: Automatically optimize window size based on observed throughput
4. **Documentation**: Add performance tuning guide to developer standards

