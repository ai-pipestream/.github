# Dev Service Discovery - Automatic Container Orchestration

## Overview

The Dev Service Discovery feature extends the existing Quarkus DevServices extension to automatically orchestrate service containers during development. This enables developers to start coding immediately with minimal setup - just run `quarkus dev` and the extension handles starting all dependencies, managing container-to-dev-mode handoffs, and cleaning up resources.

## Vision

**Developer Experience Goal**: Enable any developer to checkout any service, run `quarkus dev`, and immediately start coding with a fully functional local environment.

### Key Benefits

1. **Zero-Configuration Development**: No manual docker-compose setup, no service discovery configuration
2. **Automatic Dependency Management**: Extension starts all required services automatically
3. **Seamless Handoff**: Container instances gracefully hand off to dev mode instances
4. **Multi-Service Development**: Multiple services can run in dev mode simultaneously
5. **Module Development**: External developers can build modules in any language without understanding the full platform

## Architecture

### Current State

The existing `quarkus-pipeline-devservices` extension:
- Extracts `compose-devservices.yml` to `~/.pipeline/`
- Manages infrastructure services (Consul, MySQL, Kafka, OpenSearch, etc.)
- Uses Quarkus Compose Dev Services for container orchestration
- Provides bootstrap infrastructure for all services

### Extension Points

The new functionality extends the existing extension with:

1. **Service Container Management**: Add service containers to compose file
2. **Service Identification**: Use `quarkus.application.name` to identify which service is running
3. **Consul Tagging**: Tag services with `deployment:container` or `deployment:dev` to distinguish instances
4. **Registration Monitoring**: Query Consul to detect when services register
5. **Graceful Handoff**: Remove container instances when dev mode registers

## How It Works

### Phase 1: Platform-Registration Service (MVP)

**Startup Flow:**

1. Developer runs `quarkus dev` on `platform-registration-service`
2. Extension detects `quarkus.application.name=platform-registration-service`
3. Extension checks `compose-devservices.yml` for matching service definition
4. Extension starts all infrastructure containers (Consul, MySQL, etc.) - already working
5. Extension starts `platform-registration-service` container and all other platform containers
6. Pipestream AI Containers register in Consul with tag `deployment:container`
7. Extension waits for container registration (polls Consul API)
8. Extension starts Quarkus dev mode
9. Dev mode registers in Consul with tag `deployment:dev`
10. Extension queries Consul for container instance (by tag `deployment:container`)
11. Extension deregisters container from Consul
12. Extension stops container
13. Only dev mode instance remains registered

**Shutdown Flow:**

1. Developer stops dev mode (Ctrl+C)
2. Extension detects shutdown
3. Optionally: Restart container (future enhancement)
4. Clean up resources

### Phase 2: Multiple Foundational Services

After Phase 1 is validated, extend to:
- `mapping-service`
- `connector-admin`
- `account-service`
- `opensearch-manager`
- ... any other services we need

Each service follows the same pattern independently.

### Phase 3: Frontend Support

Frontend (Node.js) has simpler requirements:
- Only needs `platform-registration-service` running
- Can query Consul for other services once platform-registration is available
- Separate compose file or simple startup script
- No Quarkus extension needed

## Technical Implementation

### Service Identification

#### Service Name

The quarkus standard for service naming is `quarkus.application.name`.  So we can use that to identify the service we're running.

#### Self-registration Today

```properties
service.registration.enabled=true
%test.service.registration.enabled=false
service.registration.service-name=connector-intake-service
service.registration.description=Connector document ingestion, authentication, metadata enrichment, and rate limiting
service.registration.service-type=APPLICATION
service.registration.host=${CONNECTOR_INTAKE_SERVICE_HOST:host.docker.internal}
service.registration.port=${quarkus.http.port}
service.registration.capabilities=document-ingestion,metadata-enrichment,rate-limiting
service.registration.tags=connector,intake,core-service
```
The above configuration is used by the `SelfRegistrationService` to register the service `connector-intake-service` with the platform.  We just need to add to the tags for self-registration. In the docker setup, we can use the `SERVICE_REGISTRATION_TAGS` environment variable to set the tags.  Otherwise dev services can add the tag for the dev mode instance tag.

#### Implementation details

**Method**: Use `quarkus.application.name` from `application.properties`

```properties
# platform-registration-service/src/main/resources/application.properties
quarkus.application.name=platform-registration-service
```

The Pipestream AI platform already registers via consul automatically. All the services have this configuration (see `platform-registration-service/src/main/resources/application.properties` for an example)

**Extension Logic:**
```java
@ConfigProperty(name = "quarkus.application.name")
String serviceName;

// Check if service exists in compose file
if (composeFileHasService(serviceName)) {
    // Orchestrate container lifecycle
}
```

### Consul Tagging Strategy

Consul can store service metadata via tags. We will use tags to differentiate between container instances and dev mode instances.  This feature is already part of the self-registration process.



**Container Instances**: Tag with `deployment:container`
**Dev Mode Instances**: Tag with `deployment:dev`

**Implementation in Self-Registration:**

Two approaches for adding deployment tags:

**Option 1: Environment Variable (Containers)**
- Containers use `SERVICE_REGISTRATION_TAGS=deployment:container` environment variable
- This is already parsed by `SelfRegistrationService` from `service.registration.tags` config
- Compose file sets: `SERVICE_REGISTRATION_TAGS=deployment:container`

**Option 2: Dev Mode Detection (Dev Services)**
- Modify `SelfRegistrationService.buildServiceRequest()` to detect dev mode
- Add `deployment:dev` tag automatically when in dev mode
- Check for dev mode indicators (Quarkus dev profile, environment variables, etc.)

**Recommended Implementation:**
```java
// In SelfRegistrationService.buildServiceRequest()
private ServiceRegistrationRequest buildServiceRequest() {
    ServiceRegistrationRequest.Builder builder = ServiceRegistrationRequest.newBuilder()
        .setServiceName(serviceName)
        // ... other fields ...
    
    // Add existing tags from config (includes deployment:container if set via env var)
    if (!tags.isEmpty()) {
        Arrays.stream(tags.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .forEach(builder::addTags);
    }
    
    // If no deployment tag found, detect and add it
    boolean hasDeploymentTag = tags.contains("deployment:container") || 
                               tags.contains("deployment:dev");
    
    if (!hasDeploymentTag) {
        String deploymentType = detectDeploymentType(); // "container" or "dev"
        builder.addTags("deployment:" + deploymentType);
    }
    
    return builder.build();
}

private String detectDeploymentType() {
    // Check if running in Quarkus dev mode
    // Option 1: Check profile
    if ("dev".equals(profile) && !isContainer()) {
        return "dev";
    }
    
    // Option 2: Check environment variable
    String devMode = System.getenv("QUARKUS_DEV_MODE");
    if ("true".equals(devMode)) {
        return "dev";
    }
    
    // Option 3: Check system property (set by extension)
    String devServices = System.getProperty("quarkus.devservices.enabled");
    if ("true".equals(devServices)) {
        return "dev";
    }
    
    // Default to container
    return "container";
}

private boolean isContainer() {
    // Check for container-specific indicators
    return System.getenv("CONTAINER") != null 
        || Files.exists(Path.of("/.dockerenv"));
}
```

**Note**: The `service.registration.tags` config property already supports comma-separated tags, so containers can set `SERVICE_REGISTRATION_TAGS=deployment:container` and it will be included automatically.

### Container Service Definition
Production tags automatically deploy new instances of the services to docker.io. This makes it easy to use the latest version of the service without having to build locally.

Add to `compose-devservices.yml`:

```yaml
services:
  # ... existing infrastructure services ...
  
  platform-registration-service:
    image: docker.io/ai-pipestream/platform-registration-service:latest
    container_name: pipeline-platform-registration-service
    networks:
      - pipeline-test-network
    ports:
      - "38101:38101"  # gRPC port
    environment:
      - QUARKUS_APPLICATION_NAME=platform-registration-service
      - SERVICE_REGISTRATION_TAGS=deployment:container
      - CONSUL_HOST=consul
      - CONSUL_PORT=8500
    depends_on:
      consul:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "grpc_health_probe", "-addr=localhost:38101"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 30s
```

### Registration Detection

**Extension Runtime Component:**

Create new runtime recorder/processor that:
1. Injects ConsulClient (from platform-libraries)
2. Polls Consul API: `GET /v1/health/service/{serviceName}?tag=deployment:container`
3. Waits for container registration
4. Starts dev mode
5. Polls for dev mode registration: `GET /v1/health/service/{serviceName}?tag=deployment:dev`
6. Finds container instance and deregisters it
7. Stops container

**Consul Query Example:**

Using Mutiny Consul Client from `io.vertx.mutiny.ext.consul.ConsulClient`:

```java
@Inject
ConsulClient consulClient;  // io.vertx.mutiny.ext.consul.ConsulClient

// Wait for container registration
Uni<ServiceList> waitForContainerRegistration(String serviceName) {
    return consulClient
        .healthService(serviceName, true, new ServiceQueryOptions()
            .setTag("deployment:container"))
        .onFailure().retry()
            .atMost(30)
            .withDelay(Duration.ofSeconds(2))
        .onItem().invoke(services -> {
            if (services.getList().isEmpty()) {
                throw new RuntimeException("Container not registered yet");
            }
        });
}

// Wait for dev mode registration
Uni<ServiceList> waitForDevModeRegistration(String serviceName) {
    return consulClient
        .healthService(serviceName, true, new ServiceQueryOptions()
            .setTag("deployment:dev"))
        .onFailure().retry()
            .atMost(30)
            .withDelay(Duration.ofSeconds(2));
}

// Deregister container
Uni<Void> deregisterContainer(String serviceId) {
    return consulClient.deregisterService(serviceId);
}

// Find container service ID from service list
String findContainerServiceId(ServiceList services) {
    return services.getList().stream()
        .filter(service -> service.getService().getTags().contains("deployment:container"))
        .map(service -> service.getService().getId())
        .findFirst()
        .orElseThrow(() -> new RuntimeException("Container service not found"));
}
```

**Reference**: See `platform-registration-service/src/main/java/ai/pipestream/registration/consul/ConsulRegistrar.java` and `ConsulClientProducer.java` for examples of using Mutiny Consul Client.

### Extension Architecture

**New Components Needed:**

1. **Runtime Recorder** (`ServiceOrchestrationRecorder`):
   - Manages container lifecycle
   - Monitors Consul registrations
   - Handles handoff logic

2. **Build Step Processor** (`ServiceOrchestrationProcessor`):
   - Detects service name
   - Validates compose file has service definition
   - Configures orchestration

3. **Compose File Enhancement**:
   - Add service container definitions
   - Configure environment variables for tagging
   - Set up health checks

## Development Phases

### Phase 1: Platform-Registration Service + All Platform Containers (MVP)

**Goal**: Validate the pattern with platform-registration in dev mode, all other services as containers

**Key Insight**: When running `quarkus dev` on platform-registration-service, start ALL platform service containers (mapping-service, connector-admin, account-service, opensearch-manager, etc.) so the full platform is available. Only platform-registration-service itself will be replaced by dev mode.

**Tasks:**
1. Add ALL platform service containers to `compose-devservices.yml`:
   - `platform-registration-service` (will be replaced by dev mode)
   - `mapping-service`
   - `connector-admin`
   - `account-service`
   - `opensearch-manager`
   - Any other core services needed
2. Configure containers with `SERVICE_REGISTRATION_TAGS=deployment:container` environment variable
3. Enhance `SelfRegistrationService` to detect dev mode and add `deployment:dev` tag (or use `SERVICE_REGISTRATION_TAGS` env var for containers)
4. Create runtime orchestration component that:
   - Starts all platform containers
   - Only manages handoff for the service running in dev mode
5. Implement container → dev mode handoff for platform-registration-service
6. Test with platform-registration-service in dev mode, all others as containers

**Success Criteria:**
- Developer runs `quarkus dev` on platform-registration-service
- ALL platform containers start automatically
- Platform-registration container registers with `deployment:container` tag
- Dev mode starts and registers with `deployment:dev` tag
- Platform-registration container is removed, dev mode instance remains
- All other containers continue running
- Full platform is available for development
- No manual intervention required

### Phase 2: Frontend Support (Immediate Next Step)

**Goal**: Enable frontend development with minimal setup, always have frontend available

**Why Phase 2 (Not Phase 3)**: Frontend only needs platform-registration running. Once Phase 1 is complete, frontend can bootstrap immediately. This allows developers to always have a working frontend while developing backend services.

**Requirements:**
- Frontend needs platform-registration running (from Phase 1)
- Can discover other services via Consul
- Simple startup script or compose file
- No Quarkus extension needed (Node.js)

**Implementation Options:**
1. **Simple Startup Script**: Check if platform-registration is available, start if not
2. **Docker Compose**: Add frontend service to compose file with dependency on platform-registration
3. **Hybrid**: Frontend startup script that uses compose for platform-registration if needed

**Recommended Approach**: Simple Node.js startup script that:
- Checks Consul for platform-registration service
- If not found, starts it via docker-compose (or waits if dev mode is starting)
- Once available, frontend can query Consul for all other services
- Frontend starts normally

**Success Criteria:**
- Developer can start frontend with single command
- Frontend automatically ensures platform-registration is available
- Frontend can discover all services via Consul
- Works whether platform-registration is in dev mode or container

### Phase 3: Additional Services (Future)

**Goal**: Extend orchestration to other services for multi-service development

**Services to Add Orchestration For:**
- `mapping-service`
- `connector-admin`
- `account-service`
- `opensearch-manager`
- Any other services developers want to work on

**Tasks:**
1. Each service can independently run in dev mode
2. Extension handles handoff for whichever service is in dev mode
3. Other services continue as containers
4. Multiple services can be in dev mode simultaneously

**Note**: This phase is less critical since Phase 1 + 2 provide a complete development environment. Phase 3 is for developers who need to work on multiple services simultaneously.

## State Management

### States

1. **All Services Down** → Dev services down
   - Clean shutdown
   - No orphaned containers

2. **One Service Starting (All Down)**
   - Start all infrastructure containers
   - Start target service container
   - Wait for container registration
   - Start dev service
   - Wait for dev service registration
   - Remove container version
   - Other containers stay running

3. **Service Started, Goes Down (Dev Mode Crash/Stop)**
   - **Initial Implementation**: No auto-restore (keep simple)
   - **Future Enhancement**: Smart restore with timeout
     - If clean shutdown: Restore container immediately
     - If crash: Wait 30 seconds, then restore (gives time to restart dev mode)

4. **Multiple Services in Dev Mode**
   - Each service manages its own handoff independently
   - Brief overlap (container + dev mode) is acceptable
   - Consul handles multiple instances gracefully

## Configuration

### Extension Configuration

```properties
# Enable service orchestration
quarkus.pipeline-devservices.service-orchestration.enabled=true

# Service name (auto-detected from quarkus.application.name)
# quarkus.pipeline-devservices.service-orchestration.service-name=platform-registration-service

# Handoff timeout (seconds)
quarkus.pipeline-devservices.service-orchestration.handoff-timeout=60

# Auto-restore container on dev mode stop (future)
# quarkus.pipeline-devservices.service-orchestration.auto-restore=false
```

### Service Configuration

Each service needs:
```properties
# Service identification
quarkus.application.name=platform-registration-service

# Self-registration enabled
service.registration.enabled=true

# Tags will be automatically added by SelfRegistrationService
```

## Benefits

### For Core Developers

- **Faster Onboarding**: New developers can start coding immediately
- **Consistent Environment**: Same setup for everyone
- **Less Manual Work**: No docker-compose management
- **Multi-Service Development**: Easy to work on multiple services

### For Module Developers

- **Language Agnostic**: Can build modules in any language
- **Minimal Setup**: Just need platform-registration running
- **Service Discovery**: Can query Consul for available services
- **No Platform Knowledge Required**: Don't need to understand full architecture

### For the Project

- **Lower Barrier to Entry**: Attract more contributors
- **Better Developer Experience**: Focus on code, not infrastructure
- **Scalable**: Easy to add new services to orchestration
- **Maintainable**: Centralized orchestration logic

## Future Enhancements

1. **Smart Container Restore**: Auto-restore containers on dev mode crash
2. **Service Dependencies**: Handle startup ordering automatically
3. **Health Check Integration**: Wait for health checks before handoff
4. **Multi-Instance Support**: Support multiple container instances
5. **Resource Management**: Better cleanup and resource tracking
6. **Performance Monitoring**: Track handoff times and optimize

## Related Documentation

- [Quarkus DevServices Extension](../platform-libraries/devservices/README.md)
- [Service Registration Architecture](../../../platform-registration-service/README.md)
- [Consul Integration](../../../platform-libraries/README.md)

## Implementation Status

**Status**: Planning Phase

**Next Steps**:
1. ✅ Create LLM context document for implementation
2. Implement Phase 1 (platform-registration-service + all platform containers)
3. Test and validate pattern
4. Implement Phase 2 (frontend support) - enables always-on frontend
5. Extend to Phase 3 (additional services) as needed

