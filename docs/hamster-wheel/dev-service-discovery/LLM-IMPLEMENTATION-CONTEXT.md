# Dev Service Discovery - LLM Implementation Context

## Issue: Implement Automatic Service Container Orchestration for DevServices Extension

### Objective

Extend the existing `quarkus-pipeline-devservices` extension to automatically orchestrate service containers during development. When a developer runs `quarkus dev` on any service, the extension should:

1. Start all infrastructure containers (already working)
2. Start the matching service container (if defined in compose file)
3. Wait for container to register in Consul
4. Start Quarkus dev mode
5. Wait for dev mode to register in Consul
6. Gracefully remove container instance (deregister from Consul, stop container)
7. Only dev mode instance remains

### Context: Current Architecture

#### Existing DevServices Extension

**Location**: `platform-libraries/devservices/`

**Key Files**:
- `devservices-deployment/src/main/java/ai/pipestream/quarkus/devservices/deployment/PipelineDevServicesProcessor.java`
  - Build step processor
  - Extracts `compose-devservices.yml` to `~/.pipeline/`
  - Configures Quarkus Compose Dev Services
  - Logs required configuration properties

- `devservices/src/main/resources/compose-devservices.yml`
  - Infrastructure services (Consul, MySQL, Kafka, OpenSearch, etc.)
  - Currently only infrastructure, no service containers

- `devservices/src/main/java/ai/pipestream/quarkus/devservices/runtime/`
  - Runtime components (recorders, config builders)

**Current Functionality**:
- Extracts compose file on build
- Starts infrastructure containers via Quarkus Compose Dev Services
- Provides service discovery via Docker labels
- Manages version tracking and updates

#### Service Registration System

**Location**: `platform-registration-service/src/main/java/ai/pipestream/registration/`

**Key Files**:
- `startup/SelfRegistrationService.java`
  - Auto-registers service on startup
  - Builds `ServiceRegistrationRequest` from config
  - Adds tags and metadata
  - **MODIFICATION NEEDED**: Add deployment type tag

- `consul/ConsulRegistrar.java`
  - Registers/unregisters services with Consul
  - Uses `ServiceOptions` with tags and metadata
  - Configures health checks

- `handlers/ServiceRegistrationHandler.java`
  - Handles registration flow
  - Emits registration events
  - Calls `ConsulRegistrar.registerService()`

**Registration Flow**:
1. `SelfRegistrationService.onStart()` triggered on `StartupEvent`
2. Builds `ServiceRegistrationRequest` with tags/metadata
3. Calls `ServiceRegistrationHandler.registerService()`
4. Handler calls `ConsulRegistrar.registerService()`
5. Service registered in Consul with tags

#### Consul Client

**Location**: `platform-libraries/libraries/` (check for Consul client library)

**Usage**: Services inject `ConsulClient` to interact with Consul

**API Methods Needed**:
- Query services: `healthService(serviceName, passingOnly, options)`
- Deregister service: `deregisterService(serviceId)`
- Register service: `registerService(options)` (already used)

#### Service Identification

**Method**: `quarkus.application.name` from `application.properties`

**Examples**:
- `platform-registration-service/src/main/resources/application.properties`: `quarkus.application.name=platform-registration-service`
- `mapping-service/src/main/resources/application.properties`: `quarkus.application.name=mapping-service`

**Extension Access**:
```java
@ConfigProperty(name = "quarkus.application.name")
String serviceName;
```

### Implementation Requirements

#### Phase 1: Platform-Registration Service + All Platform Containers (MVP)

**Goal**: Validate pattern with platform-registration in dev mode, all other services as containers

**Key Insight**: When running `quarkus dev` on platform-registration-service, start ALL platform service containers (mapping-service, connector-admin, account-service, opensearch-manager, etc.) so the full platform is available. Only platform-registration-service itself will be replaced by dev mode.

**Tasks**:

1. **Enhance Compose File**
   - File: `platform-libraries/devservices/devservices/src/main/resources/compose-devservices.yml`
   - Add ALL platform service container definitions:
     - `platform-registration-service` (will be replaced by dev mode)
     - `mapping-service`
     - `connector-admin`
     - `account-service`
     - `opensearch-manager`
     - Any other core services needed
   - Configure environment variables for deployment tagging: `SERVICE_REGISTRATION_TAGS=deployment:container`
   - Set up health checks for each service
   - Configure dependencies (Consul must be healthy for all services)

2. **Modify Self-Registration Service**
   - File: `platform-registration-service/src/main/java/ai/pipestream/registration/startup/SelfRegistrationService.java`
   - Method: `buildServiceRequest()`
   - **Two approaches**:
     - **Containers**: Use `SERVICE_REGISTRATION_TAGS=deployment:container` environment variable (already parsed from `service.registration.tags` config)
     - **Dev Mode**: Detect dev mode and automatically add `deployment:dev` tag
   - Detection method: Check for dev mode indicators (profile, `QUARKUS_DEV_MODE` env var, `quarkus.devservices.enabled` system property)
   - If no deployment tag found in config, auto-detect and add it

3. **Create Runtime Orchestration Component**
   - New file: `platform-libraries/devservices/devservices/src/main/java/ai/pipestream/quarkus/devservices/runtime/ServiceOrchestrationRecorder.java`
   - Responsibilities:
     - Detect service name from config (`quarkus.application.name`)
     - Check if service exists in compose file
     - Start ALL platform containers (not just the one in dev mode)
     - Monitor Consul for container registration of the dev mode service
     - Wait for dev mode registration
     - Deregister only the container instance of the service in dev mode
     - Stop only that container (other containers continue running)
   - Use `@Recorder` annotation for Quarkus runtime recording
   - Use Mutiny Consul Client: `io.vertx.mutiny.ext.consul.ConsulClient`

4. **Create Build Step Processor**
   - New file: `platform-libraries/devservices/devservices-deployment/src/main/java/ai/pipestream/quarkus/devservices/deployment/ServiceOrchestrationProcessor.java`
   - Responsibilities:
     - Read `quarkus.application.name` at build time
     - Validate compose file has service definition
     - Produce build items for runtime recorder
   - Use `@BuildStep` annotation

5. **Consul Integration**
   - Inject `ConsulClient` (Mutiny version: `io.vertx.mutiny.ext.consul.ConsulClient`)
   - See `platform-registration-service/src/main/java/ai/pipestream/registration/consul/ConsulClientProducer.java` for example
   - Query Consul API using Mutiny: `consulClient.healthService(serviceName, true, new ServiceQueryOptions().setTag("deployment:container"))`
   - Poll until container registers (with timeout/retry using Mutiny retry)
   - Query for dev mode: `consulClient.healthService(serviceName, true, new ServiceQueryOptions().setTag("deployment:dev"))`
   - Deregister container: `consulClient.deregisterService(serviceId)` (returns `Uni<Void>`)

#### Technical Details

**Deployment Type Detection**:

Options for detecting dev mode:
1. Check `quarkus.devservices.enabled` property
2. Check for `quarkus.dev` profile
3. Check environment variable set by Quarkus
4. System property set by extension

**Recommended Approach**:
```java
private String detectDeploymentType() {
    // Check if running in Quarkus dev mode
    // Option 1: Profile check
    if (profile.equals("dev") && !isContainer()) {
        return "dev";
    }
    // Option 2: Environment variable
    String devMode = System.getenv("QUARKUS_DEV_MODE");
    if ("true".equals(devMode)) {
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

**Container Orchestration**:

Options:
1. Use Quarkus Compose Dev Services API (if available)
2. Direct Docker Compose API calls
3. Docker API via Java client

**Recommended**: Extend existing Quarkus Compose Dev Services integration

**Consul Query Pattern**:
```java
@Inject
ConsulClient consulClient;

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
                throw new RuntimeException("Container not registered");
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
void deregisterContainer(String serviceId) {
    consulClient.deregisterService(serviceId)
        .await().atMost(Duration.ofSeconds(10));
}
```

**Service ID Generation**:

Check `ConsulRegistrar.generateServiceId()` method to understand service ID format:
- Pattern: `{serviceName}-{host}-{port}`
- Need to match this pattern when finding container instance

### Research Tasks

Before implementing, research:

1. **Quarkus Compose Dev Services API**
   - Location: Quarkus documentation or source code
   - How to programmatically start/stop services
   - How to check service status
   - Integration points for custom orchestration

2. **Consul Client Library**
   - Location: `platform-libraries/libraries/` or dependencies
   - Available methods for querying services
   - Tag filtering capabilities
   - Deregistration methods

3. **Service Registration Flow**
   - Trace through `SelfRegistrationService` → `ServiceRegistrationHandler` → `ConsulRegistrar`
   - Understand tag/metadata flow
   - Identify best place to add deployment tag

4. **Quarkus Dev Mode Detection**
   - How Quarkus indicates dev mode at runtime
   - Environment variables set by Quarkus
   - System properties available
   - Profile information

5. **Docker Compose Integration**
   - How Quarkus Compose Dev Services manages containers
   - API for starting/stopping individual services
   - Health check integration
   - Network configuration

### Testing Strategy

1. **Unit Tests**
   - Test deployment type detection logic
   - Test Consul query methods
   - Test service ID matching

2. **Integration Tests**
   - Start platform-registration-service in dev mode
   - Verify container starts
   - Verify container registers with correct tag
   - Verify dev mode registers with correct tag
   - Verify container is removed
   - Verify only dev mode instance remains

3. **Manual Testing**
   - Run `quarkus dev` on platform-registration-service
   - Check Consul UI for registrations
   - Verify handoff happens automatically
   - Test shutdown and cleanup

### Success Criteria

Phase 1 is complete when:
- ✅ Developer runs `quarkus dev` on platform-registration-service
- ✅ Container starts automatically (no manual docker-compose)
- ✅ Container registers in Consul with `deployment:container` tag
- ✅ Dev mode starts and registers with `deployment:dev` tag
- ✅ Container is automatically removed from Consul
- ✅ Container is stopped
- ✅ Only dev mode instance remains registered
- ✅ No manual intervention required

### Files to Create/Modify

**New Files**:
1. `platform-libraries/devservices/devservices/src/main/java/ai/pipestream/quarkus/devservices/runtime/ServiceOrchestrationRecorder.java`
2. `platform-libraries/devservices/devservices-deployment/src/main/java/ai/pipestream/quarkus/devservices/deployment/ServiceOrchestrationProcessor.java`

**Modified Files**:
1. `platform-libraries/devservices/devservices/src/main/resources/compose-devservices.yml`
   - Add platform-registration-service definition

2. `platform-registration-service/src/main/java/ai/pipestream/registration/startup/SelfRegistrationService.java`
   - Add deployment type detection
   - Add deployment tag to registration

**Configuration Files**:
1. `platform-libraries/devservices/devservices-deployment/src/main/java/ai/pipestream/quarkus/devservices/PipelineDevServicesConfig.java`
   - Add service orchestration configuration properties (optional for Phase 1)

### Dependencies

**Required**:
- Consul Client (already in platform-libraries)
- Quarkus Compose Dev Services (already integrated)
- Docker/Docker Compose (runtime requirement)

**May Need**:
- Docker Java API client (if direct Docker API needed)
- Additional Quarkus extensions for runtime recording

### Next Steps After Phase 1

1. Test and validate pattern
2. Document learnings
3. **Implement Phase 2: Frontend Support** (immediate next step)
   - Frontend only needs platform-registration running
   - Simple Node.js startup script that checks Consul
   - Enables always-on frontend for development
4. **Phase 3: Additional Services** (future, less critical)
   - Extend orchestration to other services for multi-service development
   - Each service can independently run in dev mode
5. Add smart container restore (future enhancement)

### Questions to Resolve

1. **Container Image Source**: Where are service images published?
   - Answer: `docker.io/ai-pipestream/{service-name}:latest` or GitHub registry
   - Need to verify exact registry and tagging strategy

2. **Health Check Method**: How to health check gRPC services?
   - Answer: gRPC health probe or custom health check
   - Need to verify what's available in containers

3. **Service Ports**: What ports do services use?
   - Answer: Check `application.properties` for each service
   - Platform-registration: 38101 (verify)

4. **Consul Client Location**: Exact location of Consul client library
   - Answer: Research `platform-libraries/libraries/` or check dependencies

5. **Quarkus Compose Dev Services API**: How to programmatically control services?
   - Answer: Research Quarkus documentation or source code

### Related Code Locations

- DevServices Extension: `platform-libraries/devservices/`
- Service Registration: `platform-registration-service/src/main/java/ai/pipestream/registration/`
- Consul Integration: `platform-libraries/libraries/` (research exact location)
- Service Configs: `{service}/src/main/resources/application.properties`

### Implementation Notes

- Keep it simple for Phase 1 - no complex restore logic
- Focus on getting the handoff working correctly
- Use existing infrastructure (Consul, Compose Dev Services)
- Follow existing patterns in the codebase
- Add comprehensive logging for debugging
- Handle errors gracefully with clear messages

