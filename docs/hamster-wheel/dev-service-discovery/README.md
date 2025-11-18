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

Modify `SelfRegistrationService.buildServiceRequest()`:
```java
private ServiceRegistrationRequest buildServiceRequest() {
    ServiceRegistrationRequest.Builder builder = ServiceRegistrationRequest.newBuilder()
        .setServiceName(serviceName)
        // ... other fields ...
    
    // Detect deployment type
    String deploymentType = detectDeploymentType(); // "container" or "dev"
    builder.addTags("deployment:" + deploymentType);
    
    return builder.build();
}

private String detectDeploymentType() {
    // Check if running in Quarkus dev mode
    // Dev mode: quarkus.devservices.enabled or specific dev mode indicator
    // Container: default or explicit container environment
    return isDevMode() ? "dev" : "container";
}
```

### Container Service Definition

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
```java
@Inject
ConsulClient consulClient;

// Wait for container registration
Uni<ServiceList> containerServices = consulClient
    .healthService(serviceName, true, new ServiceQueryOptions()
        .setTag("deployment:container"))
    .onFailure().retry().atMost(30).withDelay(Duration.ofSeconds(2));

// Wait for dev mode registration
Uni<ServiceList> devServices = consulClient
    .healthService(serviceName, true, new ServiceQueryOptions()
        .setTag("deployment:dev"))
    .onFailure().retry().atMost(30).withDelay(Duration.ofSeconds(2));

// Deregister container
String containerServiceId = findContainerServiceId(containerServices);
consulClient.deregisterService(containerServiceId);
```

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

### Phase 1: Platform-Registration Service (MVP)

**Goal**: Validate the pattern with a single service

**Tasks:**
1. Add `platform-registration-service` container to `compose-devservices.yml`
2. Enhance `SelfRegistrationService` to add deployment tags
3. Create runtime orchestration component
4. Implement container → dev mode handoff
5. Test with platform-registration-service

**Success Criteria:**
- Developer runs `quarkus dev` on platform-registration-service
- Container starts automatically
- Dev mode starts and replaces container
- No manual intervention required

### Phase 2: Foundational Services

**Goal**: Extend to other core services

**Services to Add:**
- `mapping-service`
- `connector-admin`
- `account-service`
- `opensearch-manager` (optional)

**Tasks:**
1. Add each service container to compose file
2. Test multi-service scenarios
3. Validate independent service orchestration

### Phase 3: Frontend Support

**Goal**: Enable frontend development with minimal setup

**Requirements:**
- Frontend only needs platform-registration running
- Can discover other services via Consul
- Simple startup script or compose file

**Implementation:**
- Create separate compose file for frontend
- Or enhance existing compose with frontend service
- Frontend startup checks for platform-registration
- Once available, frontend can query Consul for other services

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
1. Create LLM context document for implementation
2. Implement Phase 1 (platform-registration-service)
3. Test and validate pattern
4. Extend to Phase 2 (foundational services)
5. Implement Phase 3 (frontend support)

