# Unified Server Mode Limitations - HTTP/2 Flow Control Window

## Issue

When using `quarkus.grpc.server.use-separate-server=false` (unified server mode), the gRPC server uses the main Vert.x HTTP server. However, **there is no way to configure the HTTP/2 flow control window** for the unified server.

## Investigation Results

### What We Tried

1. **GrpcServerOptions Customization**: The `ServerBuilderCustomizer.customize(GrpcServerOptions)` method is called, but `GrpcServerOptions` does not expose HTTP/2 flow control window settings.

2. **Reflection Attempts**: We tried using reflection to find methods like:
   - `setInitialSettings(Http2Settings)`
   - `setHttp2InitialWindowSize(int)`
   - Any method containing "http2", "initial", "flow", or "window"
   
   **Result**: No such methods exist on `GrpcServerOptions`.

3. **Main HTTP Server Configuration**: The unified server uses the main Vert.x HTTP server, but Quarkus does not expose:
   - `quarkus.http.http2.initial-window-size` property
   - `HttpServerOptionsCustomizer` interface
   - Any other mechanism to customize the main HTTP server's HTTP/2 settings

### What Quarkus Exposes

- **Separate Server Mode** (`use-separate-server=true`): ✅ Full control via `NettyServerBuilder.initialFlowControlWindow()`
- **Unified Server Mode** (`use-separate-server=false`): ❌ No way to configure HTTP/2 flow control window

## Impact

- **Default Flow Control Window**: 64KB
- **Observed Throughput**: 5-10 MB/s for large messages
- **Required Throughput**: 250-370 MB/s (achieved with 100MB window in separate server mode)
- **Performance Impact**: **50-70x slower** for large messages in unified mode

## Recommendation

**Use `quarkus.grpc.server.use-separate-server=true`** for services that need high throughput with large messages.

### Why Separate Server Mode?

1. ✅ **Proven Performance**: 367 MB/s for 250MB messages (tested)
2. ✅ **Direct Configuration**: `NettyServerBuilder.initialFlowControlWindow()` works
3. ✅ **Full Control**: All HTTP/2 settings are configurable
4. ⚠️ **Trade-off**: Uses a separate port (but this is acceptable for gRPC services)

### When Unified Mode is Acceptable

- Small messages (< 1MB)
- Low throughput requirements (< 10 MB/s)
- Services that must share the HTTP port with REST endpoints

## Potential Solutions (Future)

1. **Quarkus Enhancement**: Request that Quarkus expose HTTP/2 flow control window configuration for unified server mode
2. **Vert.x HTTP Server Customizer**: If Quarkus adds an `HttpServerOptionsCustomizer` interface, we could use it
3. **Configuration Property**: If Quarkus adds `quarkus.http.http2.initial-window-size`, it would automatically apply to unified gRPC server

## Forum Post Template

```
Subject: Cannot configure HTTP/2 flow control window in unified gRPC server mode

When using quarkus.grpc.server.use-separate-server=false, the gRPC server 
uses the main Vert.x HTTP server. However, there's no way to configure the 
HTTP/2 flow control window for this unified server.

The default 64KB window causes severe performance issues (5-10 MB/s vs 
250-370 MB/s with 100MB window) for large messages.

Is there a way to:
1. Configure HTTP/2 initial window size for the main HTTP server?
2. Customize HttpServerOptions used by the unified gRPC server?
3. Access the underlying Vert.x HTTP server to set HTTP/2 settings?

If not, could this be added as a feature? The separate server mode works 
great, but unified mode would be preferred if we could configure flow control.
```

## Current Implementation

Our `GrpcServerFlowControlCustomizer`:
- ✅ Works perfectly with `use-separate-server=true`
- ⚠️ Logs info message in unified mode (cannot configure)
- 📝 Documents the limitation clearly

## Conclusion

**Use `use-separate-server=true`** for services requiring high throughput. This is a known limitation of Quarkus's unified server mode.

### Decision

After investigation, we determined that:
1. `HttpServerOptionsCustomizer` exists but does not reliably work for gRPC traffic in unified mode
2. The unified server abstraction hides the necessary HTTP/2 configuration points
3. Separate server mode (Netty-based) provides full control and proven performance
4. The trade-off of a separate port is acceptable for gRPC services

**Netty-based separate server mode is the recommended approach** for high-performance gRPC services.

