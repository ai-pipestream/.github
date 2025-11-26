# Kafka & Apicurio Guide

## Introduction

This guide provides the standard, platform-approved method for configuring and testing Kafka producers and consumers within a Quarkus application using the Apicurio Schema Registry. Following this guide ensures that your services adhere to the critical platform standards of using **UUID keys** and **Protobuf values** for all Kafka messages.

### Why Apicurio?

The Apicurio Schema Registry is used to manage schemas for our Protobuf messages. This is not optional; it is a core part of the platform. It prevents serialization issues, allows for safe schema evolution over time, and provides a central source of truth for our event-driven architecture. When a message's schema is updated, that change is recorded in the registry, allowing us to handle different message versions without breaking consumers.

### The Zero-Config Kafka Extension

The Pipeline Platform includes a **Quarkus extension** (`pipeline-kafka-quarkus-extension`) that automatically handles all Kafka configuration. This extension provides:

- **Automatic topic mapping** from channel names to Kafka topics
- **Automatic connector configuration** for producers and consumers
- **Automatic Protobuf deserialization** with correct return types
- **Zero manual configuration** - just use channel names in your code

The extension enforces platform standards (UUID keys, Protobuf values, Apicurio integration) without requiring developers to configure serializers, deserializers, or registry URLs.

## What You Need to Do (Quick Start)

**To add Kafka messaging to your Quarkus service:**

1. **Add dependency:**
   ```groovy
   implementation 'ai.pipestream:pipeline-kafka-quarkus-extension'
   ```

2. **Configure infrastructure URLs:**
   ```properties
   kafka.bootstrap.servers=${KAFKA_BOOTSTRAP_SERVERS}
   mp.messaging.connector.smallrye-kafka.apicurio.registry.url=${APICURIO_REGISTRY_URL}
   ```

3. **Use channel names in your code:**
   ```java
   @Channel("my-events-producer") MutinyEmitter<MyEvent> emitter;
   @Incoming("my-events-consumer") ConsumerRecord<UUID, MyEvent> consume(record);
   ```

**That's it!** The extension handles all the complex Kafka configuration automatically.

## Part 1: The Test Environment Foundation

All services that use Kafka must be tested against a real Kafka and Apicurio instance. We do not use in-memory messaging. The standard way to manage this is with a `docker-compose.yml` file located in `src/test/resources/`.

The `quarkus-compose-devservices` extension automatically discovers this file and manages the lifecycle of the services defined within it, making them available to your tests.

**`src/test/resources/docker-compose.yml`**
(A complete, standard test environment for Kafka, Apicurio, and MySQL)
```yaml
version: '3.8'
services:
  mysql-test:
    image: mysql:8.0
    # ... (service details for mysql) ...
  init-db:
    image: mysql:8.0
    # ... (service details for init-db) ...
  kafka-test:
    image: redpandadata/redpanda:latest
    # ... (service details for redpanda/kafka) ...
  apicurio-registry-test:
    image: apicurio/apicurio-registry:3.1.2
    # ... (service details for apicurio) ...
```
*(**Note:** For brevity, the full YAML is not shown here but should be the standard test environment file used across projects.)*

## Part 2: Dependencies

Add the Pipeline Kafka extension to your `build.gradle`:

```groovy
dependencies {
    // Pipeline BOM for consistent versions
    implementation platform('ai.pipestream:pipeline-bom:0.2.10') //or later

    // The Kafka extension - handles all configuration automatically
    implementation 'ai.pipestream:pipeline-kafka-quarkus-extension'

    // Your Protobuf message types
    implementation 'ai.pipestream:grpc-stubs'
}
```

## Part 3: `application.properties` Configuration

The Pipeline Kafka extension automates most configuration, but you still need to configure your infrastructure endpoints.

### Required Infrastructure Configuration

```properties
# --- REQUIRED: Infrastructure Settings ---
# Kafka bootstrap servers (required for all environments)
kafka.bootstrap.servers=${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}

# Apicurio Registry URL (required for all environments)
mp.messaging.connector.smallrye-kafka.apicurio.registry.url=${APICURIO_REGISTRY_URL:http://localhost:8082/apis/registry/v3}
```

The extension automatically handles:
- ✅ UUID keys and Protobuf values
- ✅ Apicurio Registry integration settings
- ✅ Platform reliability and performance settings
- ✅ Automatic topic mapping from channel names
- ✅ Connector configuration for detected channels
- ✅ Return-class inference for deserialization

### Optional: Custom Topic Mapping

If you need a channel to use a different topic name, override the automatic mapping:

```properties
# Example: channel "internal-events" should use topic "public-events-v1"
mp.messaging.outgoing.internal-events.topic=public-events-v1
mp.messaging.incoming.internal-events.topic=public-events-v1
```

## Part 4: Application Code

### Automatic Topic Mapping

The extension automatically maps channel names to Kafka topics. For producer/consumer pairs, use directional suffixes that map to the same topic:

- `@Channel("validation-events-producer")` → produces to topic `"validation-events"`
- `@Incoming("validation-events-consumer")` → consumes from topic `"validation-events"`

**Note:** SmallRye Reactive Messaging prevents using the same channel name for both @Channel (producer) and @Incoming (consumer) simultaneously. This conflict only occurs when you have BOTH directions configured with identical names. Single-direction usage (producer-only or consumer-only) works fine.

### Producer Code (`ValidationEventPublisher.java`)

The producer code is simple. Just inject an `Emitter` with your channel name. The extension handles all serialization automatically.

```java
import io.pipeline.validation.ValidationEvent;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Channel;
import io.smallrye.reactive.messaging.MutinyEmitter;

@ApplicationScoped
public class ValidationEventPublisher {

    @Channel("validation-events-producer")  // Automatically maps to topic "validation-events"
    MutinyEmitter<ValidationEvent> emitter;

    public Uni<Void> publishValidationResult(ValidationEvent event) {
        // The extension automatically:
        // - Uses UUID keys
        // - Serializes ValidationEvent as Protobuf
        // - Registers schema with Apicurio
        return emitter.send(event);
    }
}
```

### Consumer Code (`ValidationEventConsumer.java`)

The consumer uses the standard `ConsumerRecord<UUID, YourProtobufType>` signature. The extension automatically configures the Apicurio deserializer with the correct return type.

```java
import io.pipeline.validation.ValidationEvent;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import java.util.UUID;

@ApplicationScoped
public class ValidationEventConsumer {

    @Incoming("validation-events-consumer")  // Automatically maps to topic "validation-events"
    public Uni<Void> consume(ConsumerRecord<UUID, ValidationEvent> record) {
        UUID messageKey = record.key();
        ValidationEvent event = record.value();

        LOG.infof("Received validation event with key %s: %s",
                 messageKey, event.getMessage());

        // The extension automatically:
        // - Deserializes UUID keys
        // - Deserializes ValidationEvent from Protobuf
        // - Handles schema evolution via Apicurio
        return processValidationEvent(event);
    }
}
```

## Part 5: Testing

Testing against real Kafka/Apicurio services is required. The extension simplifies testing by handling all the complex configuration automatically.

### Test Infrastructure Setup

**`src/test/resources/compose-test-services.yml`** (same as before)
```yaml
version: '3.8'
services:
  kafka-test:
    image: redpandadata/redpanda:latest
    # ... standard kafka configuration ...
  apicurio-registry-test:
    image: apicurio/apicurio-registry:3.0.12
    # ... standard apicurio configuration ...
```

**`src/test/resources/application.properties`**
```properties
# Enable test infrastructure
%test.quarkus.compose.devservices.enabled=true
%test.quarkus.compose.devservices.files=src/test/resources/compose-test-services.yml

# Infrastructure URLs (provided by compose-devservices)
%test.kafka.bootstrap.servers=${KAFKA_BOOTSTRAP_SERVERS:localhost:9095}
%test.mp.messaging.connector.smallrye-kafka.apicurio.registry.url=${APICURIO_REGISTRY_URL:http://localhost:8082/apis/registry/v3}

# The extension automatically handles ALL other Kafka config!
# No manual serializers, deserializers, connector settings, etc. needed!
```

### How to Test a PRODUCER

The extension makes producer testing much simpler - you don't need to manually configure serializers anymore.

```java
@QuarkusTest
public class AccountEventPublisherTest {

    @Inject
    AccountEventPublisher publisher;  // Your service with @Channel injection

    @Test
    public void testAccountCreatedEventIsPublished() {
        // ARRANGE
        AccountEvent event = AccountEvent.newBuilder()...build();

        // ACT: Call your service method
        publisher.publishAccountCreatedEvent(event);

        // ASSERT: The extension handles all the complex Kafka setup automatically
        // You can use @Inject to get a test consumer if needed, or verify via downstream effects
    }
}
```

### How to Test a CONSUMER

Consumer testing is also simplified - the extension handles deserializer configuration automatically.

```java
@QuarkusTest
public class DriveUpdateConsumerTest {

    @InjectMock
    OpenSearchIndexingService indexingService; // Mock downstream services

    @Test
    public void testConsumer_indexesDrive() {
        // ARRANGE: Mock the downstream behavior
        when(indexingService.indexDrive(any())).thenReturn(Uni.createFrom().voidItem());

        // ACT: The consumer will automatically receive messages from the test topic
        // The extension configures the consumer with correct deserialization

        DriveUpdateNotification expectedNotification = DriveUpdateNotification.newBuilder()...build();

        // Send test message (you can use a test producer utility)
        sendTestMessage("drive-updates", expectedNotification);

        // ASSERT: Verify your consumer called the mocked service
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(indexingService, times(1)).indexDrive(expectedNotification);
        });
    }
}
```

## Final Best Practices

1.  **Add the Extension Dependency.** Include `ai.pipestream:pipeline-kafka-quarkus-extension` in your build.gradle - it handles most configuration automatically.

2.  **Configure Infrastructure URLs.** Always set `kafka.bootstrap.servers` and `apicurio.registry.url` for all environments.

3.  **Use Directional Channel Names.** Use directional suffixes for producer/consumer pairs to avoid SmallRye conflicts:
    - ✅ `validation-events-producer` + `validation-events-consumer` (both map to `validation-events` topic)
    - ⚠️ Single-direction usage (producer-only or consumer-only) works fine with any channel name
    - ❌ Don't use identical channel names for both producer AND consumer simultaneously

4.  **The Standard is Automatic.** The extension enforces UUID keys, Protobuf values, and Apicurio integration - you don't configure serializers/deserializers manually.

5.  **Test Against Real Services.** Use `compose-devservices` with real Kafka/Apicurio containers for all tests.

6.  **Consumer Signature is Critical.** Always use `ConsumerRecord<UUID, YourProtobufType>` - the extension infers the return class automatically.

7.  **Don't Touch Low-Level Config.** Don't manually configure serializers, deserializers, or connector settings - the extension handles these automatically.

## Troubleshooting

### My messages aren't being sent/received

**Check:**
1. **Infrastructure URLs configured?** Verify `kafka.bootstrap.servers` and `apicurio.registry.url` are set
2. **Channel names conflict?** Don't use the same channel name for both `@Channel` and `@Incoming`
3. **Extension dependency added?** Make sure `pipeline-kafka-quarkus-extension` is in your `build.gradle`

### Getting serialization/deserialization errors

**Check:**
1. **Using Protobuf messages?** The extension only works with Protobuf messages from `grpc-stubs`
2. **Correct consumer signature?** Use `ConsumerRecord<UUID, YourProtobufType>` for consumers
3. **Extension applied?** The extension must be able to detect your `@Channel` and `@Incoming` annotations

### Build-time errors about connectors

**Check:**
1. **Channel names correct?** Use directional suffixes (e.g., `-producer`, `-consumer`)
2. **No conflicting channels?** Same channel name cannot be used for both producer and consumer

### Need different topic names

**Override automatic mapping:**
```properties
mp.messaging.outgoing.my-channel.topic=custom-topic-name
mp.messaging.incoming.my-channel.topic=custom-topic-name
```

### Still having issues?

1. Check that `pipeline-commons` and `grpc-stubs` are also in your dependencies
2. Verify your Protobuf classes are in the `ai.pipestream.*` package
3. Run `./gradlew build` to see build-time extension logs
4. Check runtime logs for Kafka connection issues

## Important: What NOT to Configure

❌ **DO NOT manually configure:**
- Kafka serializers/deserializers
- Apicurio registry settings (except the URL)
- Connector configurations
- Schema registration settings

✅ **ONLY configure:**
- `kafka.bootstrap.servers`
- `mp.messaging.connector.smallrye-kafka.apicurio.registry.url`
- Optional custom topic mappings
- Your channel names in code

The extension handles all platform-standard Kafka configuration automatically. Manual configuration will conflict with the extension.

## Summary: How the Extension Works

When you add `@Channel("my-events-producer")` and `@Incoming("my-events-consumer")` to your code:

1. **Build-time:** The extension detects your annotations and generates automatic configuration
2. **Runtime:** The extension applies platform standards:
   - UUID keys with automatic key generation
   - Protobuf serialization/deserialization
   - Apicurio schema registration and validation
   - Reliable producer/consumer settings
   - Automatic topic mapping from channel names

**Result:** You write minimal code and configuration, but get production-ready, standards-compliant Kafka messaging automatically.

