# Kafka & Apicurio Guide

## Introduction

This guide provides the standard, platform-approved method for configuring and testing Kafka producers and consumers within a Quarkus application using the Apicurio Schema Registry. Following this guide ensures that your services adhere to the critical platform standards of using **UUID keys** and **Protobuf values** for all Kafka messages.

### Why Apicurio?

The Apicurio Schema Registry is used to manage schemas for our Protobuf messages. This is not optional; it is a core part of the platform. It prevents serialization issues, allows for safe schema evolution over time, and provides a central source of truth for our event-driven architecture. When a message's schema is updated, that change is recorded in the registry, allowing us to handle different message versions without breaking consumers.

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
    image: apicurio/apicurio-registry:3.0.11
    # ... (service details for apicurio) ...
```
*(**Note:** For brevity, the full YAML is not shown here but should be the standard test environment file used across projects.)*

## Part 2: `application.properties` Configuration (The Modern Standard)

The Pipeline Platform uses a centralized `PipelineKafkaConfigSource` from the `pipeline-commons` library. This means you **do not** need to manually define serializers, deserializers, or the Apicurio URL. These are configured globally to enforce the platform standard.

Your channel configuration should be minimal and clean.

**Producer Requirements (Example):**
```properties
# --- For an outgoing channel named "account-events" ---
# 1. Define the connector (this enables all the platform defaults)
mp.messaging.outgoing.account-events.connector=smallrye-kafka

# 2. Define the topic
mp.messaging.outgoing.account-events.topic=account-events
```
That's it. The `UUIDSerializer` for the key and `ProtobufKafkaSerializer` for the value are applied automatically.

**Consumer Requirements (Example):**
```properties
# --- For an incoming channel named "drive-updates-in" ---
# 1. Define the connector
mp.messaging.incoming.drive-updates-in.connector=smallrye-kafka

# 2. Define the topic
mp.messaging.incoming.drive-updates-in.topic=drive-updates
```
**CRITICAL:** When you write your consumer code, you must use the type `ConsumerRecord<UUID, YourProtobufClass>` to correctly receive the UUID key and the deserialized Protobuf message.

## Part 3: Application Code

### Producer Code (`AccountEventPublisher.java`)

The producer code is simple. You inject an `Emitter` for your specific Protobuf type. The framework handles key generation and serialization.
```java
import io.pipeline.repository.account.AccountEvent;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

@ApplicationScoped
public class AccountEventPublisher {

    @Channel("account-events")
    Emitter<AccountEvent> emitter;

    public void publishAccountCreatedEvent(AccountEvent event) {
        // The channel name "account-events" links this emitter
        // to the properties in application.properties.
        emitter.send(event).subscribe().with(
            success -> LOG.infof("Message sent successfully!"),
            failure -> LOG.errorf(failure, "Failed to send message")
        );
    }
}
```

### Consumer Code (`DriveUpdateConsumer.java`)
The consumer signature is the most critical part of adhering to the standard.

```java
import io.pipeline.repository.filesystem.DriveUpdateNotification;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import java.util.UUID;

@ApplicationScoped
public class DriveUpdateConsumer {

    @Incoming("drive-updates-in")
    public Uni<Void> consume(ConsumerRecord<UUID, DriveUpdateNotification> record) {
        UUID messageKey = record.key();
        DriveUpdateNotification notification = record.value();

        LOG.infof("Received event with key %s for drive: %s", messageKey, notification.getDrive().getName());

        // Your business logic returns a Uni.
        // The framework handles message acknowledgment on success.
        return doBusinessLogic(notification);
    }
}
```

## Part 4: Testing

Testing against real services is required. This section shows the standard patterns for testing producers and consumers.

### How to Test a PRODUCER

To test a producer, you create a manual `KafkaConsumer` in your test to verify that the correct message was sent to the topic.
```java
@QuarkusTest
public class AccountEventPublisherTest {

    @GrpcClient("account-manager") // Example: your service is a gRPC service
    AccountServiceGrpc.AccountServiceBlockingStub accountService;

    // Inject config to build the test consumer
    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;
    @ConfigProperty(name = "mp.messaging.connector.smallrye-kafka.apicurio.registry.url")
    String apicurioRegistryUrl;

    // This helper creates a manual consumer for testing purposes.
    // Because it's manual, it needs full configuration.
    private KafkaConsumer<UUID, AccountEvent> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, UUIDDeserializer.class.getName()); // Correct
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ProtobufKafkaDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("apicurio.registry.url", apicurioRegistryUrl);
        props.put("apicurio.registry.deserializer.value.return-class", AccountEvent.class.getName()); // Correct
        return new KafkaConsumer<>(props);
    }

    @Test
    public void testAccountCreatedEventIsPublished() {
        try (KafkaConsumer<UUID, AccountEvent> consumer = createConsumer()) {
            consumer.subscribe(Collections.singletonList("account-events"));

            // ACT: Call the service method that triggers the producer
            accountService.createAccount(...);

            // ASSERT: Use Awaitility to poll for the specific message
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                ConsumerRecords<UUID, AccountEvent> records = consumer.poll(Duration.ofMillis(100));
                // ... logic to find your specific record ...
                assertNotNull(foundRecord);
            });
        }
    }
}
```

### How to Test a CONSUMER

To test a consumer, you create a manual `KafkaProducer` to send a test message. You then `@InjectMock` any downstream service your consumer calls and verify that the mock was invoked correctly.

```java
@QuarkusTest
public class DriveUpdateConsumerTest {

    @InjectMock
    OpenSearchIndexingService indexingService; // Mock the downstream service

    // Inject config to build the test producer
    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;
    @ConfigProperty(name = "mp.messaging.connector.smallrye-kafka.apicurio.registry.url")
    String apicurioRegistryUrl;

    // This helper creates a manual producer for testing purposes.
    private KafkaProducer<UUID, Object> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, UUIDSerializer.class.getName()); // Correct
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProtobufKafkaSerializer.class.getName());
        props.put("apicurio.registry.url", apicurioRegistryUrl);
        return new KafkaProducer<>(props);
    }

    @Test
    public void testConsumer_indexesDrive() {
        // ARRANGE: Mock the service behavior
        when(indexingService.indexDrive(any())).thenReturn(Uni.createFrom().voidItem());
        DriveUpdateNotification notification = DriveUpdateNotification.newBuilder()...build();
        
        // Use the standard factory to generate the UUID key
        UUID key = KafkaProtobufKeys.uuid(notification);

        // ACT: Send the message
        try (KafkaProducer<UUID, Object> producer = createProducer()) {
            producer.send(new ProducerRecord<>("drive-updates", key, notification));
        }

        // ASSERT: Use Awaitility to wait until the mock is called
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(indexingService, Mockito.times(1)).indexDrive(any());
        });
    }
}
```

## Final Best Practices

1.  **The Standard is Not Optional.** All Kafka messages **MUST** use a `UUID` key and a specific, compiled `Protobuf` message value.
2.  **Use `compose-devservices` for All Tests.** All Kafka-related tests **MUST** run against a real Kafka and Apicurio instance provided by the Docker Compose file. In-memory messaging is not an approved alternative.
3.  **`ConsumerRecord<UUID, T>` is the Contract.** Your consumer method signature is the primary enforcement of the standard.
4.  **Test Producers and Consumers Correctly.**
    -   When testing a **producer**, you create a manual **consumer** to verify its output.
    -   When testing a **consumer**, you create a manual **producer** to provide its input, and you **mock** its downstream dependencies.

