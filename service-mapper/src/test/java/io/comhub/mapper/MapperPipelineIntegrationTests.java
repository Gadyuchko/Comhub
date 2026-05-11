package io.comhub.mapper;

import io.comhub.common.config.AttributeMapping;
import io.comhub.common.config.CanonicalFieldMapping;
import io.comhub.common.config.CanonicalMapping;
import io.comhub.common.config.ConfigDiscriminator;
import io.comhub.common.config.ConfigKey;
import io.comhub.common.config.DiscriminatorSource;
import io.comhub.common.config.MappingConfig;
import io.comhub.common.config.OperationsConfig;
import io.comhub.common.event.CanonicalEvent;
import io.comhub.common.event.EventHeaders;
import io.comhub.common.event.Severity;
import io.comhub.common.kafka.JsonKafkaDeserializer;
import io.comhub.common.kafka.JsonKafkaSerializer;
import io.comhub.mapper.kafka.SourceListenerManager;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Boots the mapper service against an embedded Kafka broker and exercises the
 * full source-event-to-canonical pipeline: config publish → listener registration →
 * source event flow → canonical produce (or DLQ on failure). Each test uses its
 * own source topic so configs don't collide between tests.
 *
 * @author Roman Hadiuchko
 */
class MapperPipelineIntegrationTests {

    private static final String CONFIG_TOPIC = "comhub.config.v1";
    private static final String CANONICAL_TOPIC = "comhub.canonical.v1";
    private static final String DLQ_TOPIC = "comhub.dlq.v1";

    private static final String TOPIC_HAPPY = "test-pipeline-happy.v1";
    private static final String TOPIC_UNMATCHED = "test-pipeline-unmatched.v1";
    private static final String TOPIC_FAILURE = "test-pipeline-failure.v1";
    private static final String TOPIC_DYNAMIC = "test-pipeline-dynamic.v1";
    private static final String TOPIC_TOMBSTONE = "test-pipeline-tombstone.v1";
    private static final String TOPIC_PEER_A = "test-pipeline-peer-a.v1";
    private static final String TOPIC_PEER_B = "test-pipeline-peer-b.v1";

    private static final Duration TIMEOUT = Duration.ofSeconds(15);
    private static final Duration QUIET_WINDOW = Duration.ofSeconds(2);

    private static EmbeddedKafkaBroker broker;
    private static ConfigurableApplicationContext context;
    private static SourceListenerManager sourceListenerManager;

    @BeforeAll
    static void setUp() throws Exception {
        broker = new EmbeddedKafkaKraftBroker(1, 1,
                CONFIG_TOPIC, CANONICAL_TOPIC, DLQ_TOPIC,
                TOPIC_HAPPY, TOPIC_UNMATCHED, TOPIC_FAILURE,
                TOPIC_DYNAMIC, TOPIC_TOMBSTONE, TOPIC_PEER_A, TOPIC_PEER_B);
        broker.afterPropertiesSet();

        SpringApplication app = new SpringApplication(MapperApplication.class);
        app.setWebApplicationType(WebApplicationType.NONE);
        context = app.run(
                "--spring.kafka.bootstrap-servers=" + broker.getBrokersAsString(),
                "--spring.application.name=service-mapper",
                "--kafka.config-replay.startup-timeout=20s",
                "--kafka.group.config-replay=test-pipeline-config-" + UUID.randomUUID(),
                "--kafka.topics.config-replay=" + CONFIG_TOPIC,
                "--kafka.topics.canonical=" + CANONICAL_TOPIC,
                "--kafka.topics.dlq=" + DLQ_TOPIC,
                "--spring.kafka.consumer.auto-offset-reset=earliest");

        sourceListenerManager = context.getBean(SourceListenerManager.class);
    }

    @AfterAll
    static void tearDown() {
        if (context != null) {
            context.close();
        }
        if (broker != null) {
            broker.destroy();
        }
    }

    @Test
    void mappedEventReachesCanonicalTopicWithExpectedHeaders() throws Exception {
        publishConfig(TOPIC_HAPPY, "payment.failed",
                new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type"),
                fullMapping());
        awaitTrue(() -> sourceListenerManager.hasListenerFor(TOPIC_HAPPY));

        String sourcePayload = """
                {"type":"payment.failed","occurredAt":"2026-05-08T09:00:00Z","severity":"error",
                "category":"payments","subject":"Payment failed","message":"Card declined",
                "payload":{"customerId":"cust-123"}}
                """;
        publishSource(TOPIC_HAPPY, "order-1", sourcePayload);

        ConsumerRecord<String, CanonicalEvent> canonical = pollFirstCanonicalFor(TOPIC_HAPPY);

        CanonicalEvent event = canonical.value();
        assertThat(event.sourceTopic()).isEqualTo(TOPIC_HAPPY);
        assertThat(event.sourceEventType()).isEqualTo("payment.failed");
        assertThat(event.severity()).isEqualTo(Severity.ERROR);
        assertThat(event.subject()).isEqualTo("Payment failed");
        assertThat(event.message()).isEqualTo("Card declined");
        assertThat(event.attributes()).containsEntry("customerId", "cust-123");
        assertThat(event.rawPayload()).contains("\"type\":\"payment.failed\"");

        assertThat(canonical.key()).isEqualTo(event.id().toString());
        assertHeader(canonical, EventHeaders.EVENT_ID, event.id().toString());
        assertHeader(canonical, EventHeaders.SOURCE_TOPIC, TOPIC_HAPPY);
        assertHeader(canonical, EventHeaders.SOURCE_EVENT_TYPE, "payment.failed");
        assertHeader(canonical, EventHeaders.SOURCE_PARTITION, String.valueOf(canonical.partition()));
        assertHeader(canonical, EventHeaders.ATTEMPT_COUNT, "0");
        assertThat(canonical.headers().lastHeader(EventHeaders.SOURCE_OFFSET)).isNotNull();
    }

    @Test
    void unmatchedSourceEventIsAckedSilently() throws Exception {
        publishConfig(TOPIC_UNMATCHED, "order.created",
                new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type"),
                fullMapping());
        awaitTrue(() -> sourceListenerManager.hasListenerFor(TOPIC_UNMATCHED));

        publishSource(TOPIC_UNMATCHED, "order-x",
                "{\"type\":\"order.totally.unknown\",\"severity\":\"info\"}");

        // Both canonical and DLQ should stay empty for this source topic.
        assertNoRecordFor(CANONICAL_TOPIC, TOPIC_UNMATCHED);
        assertNoRecordFor(DLQ_TOPIC, TOPIC_UNMATCHED);
    }

    @Test
    void mappingFailureGoesToDlq() throws Exception {
        publishConfig(TOPIC_FAILURE, "payment.failed",
                new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type"),
                fullMapping());
        awaitTrue(() -> sourceListenerManager.hasListenerFor(TOPIC_FAILURE));

        String badPayload = "{\"type\":\"payment.failed\",\"severity\":\"NOT_A_REAL_SEVERITY\"}";
        publishSource(TOPIC_FAILURE, "order-bad", badPayload);

        ConsumerRecord<String, byte[]> dlqRecord = pollFirstDlqFor(TOPIC_FAILURE);

        assertThat(new String(dlqRecord.value(), StandardCharsets.UTF_8)).isEqualTo(badPayload);
        assertHeader(dlqRecord, EventHeaders.FAILURE_STAGE, "mapper");
        assertHeader(dlqRecord, EventHeaders.SOURCE_TOPIC, TOPIC_FAILURE);
        String reason = headerAsString(dlqRecord, EventHeaders.FAILURE_REASON);
        assertThat(reason).contains("severity_parse_error");
    }

    @Test
    void dynamicRegistrationStartsListenerForNewTopic() throws Exception {
        assertThat(sourceListenerManager.hasListenerFor(TOPIC_DYNAMIC)).isFalse();

        publishSource(TOPIC_DYNAMIC, "early-1", "{\"type\":\"charge.declined\"}");

        publishConfig(TOPIC_DYNAMIC, "charge.declined",
                new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type"),
                fullMapping());
        awaitTrue(() -> sourceListenerManager.hasListenerFor(TOPIC_DYNAMIC));

        publishSource(TOPIC_DYNAMIC, "late-1", "{\"type\":\"charge.declined\",\"severity\":\"warning\"}");

        ConsumerRecord<String, CanonicalEvent> canonical = pollFirstCanonicalFor(TOPIC_DYNAMIC);
        assertThat(canonical.value().sourceEventType()).isEqualTo("charge.declined");
    }

    @Test
    void tombstoneStopsLastConfigListener() throws Exception {
        publishConfig(TOPIC_TOMBSTONE, "only.one",
                new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type"),
                fullMapping());
        awaitTrue(() -> sourceListenerManager.hasListenerFor(TOPIC_TOMBSTONE));

        publishTombstone(TOPIC_TOMBSTONE, "only.one");
        awaitTrue(() -> !sourceListenerManager.hasListenerFor(TOPIC_TOMBSTONE));

        publishSource(TOPIC_TOMBSTONE, "after-stop", "{\"type\":\"only.one\"}");

        assertNoRecordFor(CANONICAL_TOPIC, TOPIC_TOMBSTONE);
        assertNoRecordFor(DLQ_TOPIC, TOPIC_TOMBSTONE);
    }

    @Test
    void peerAddDoesNotTouchOtherListeners() throws Exception {
        publishConfig(TOPIC_PEER_A, "type.alpha",
                new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type"),
                fullMapping());
        publishConfig(TOPIC_PEER_B, "type.gamma",
                new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type"),
                fullMapping());
        awaitTrue(() -> sourceListenerManager.hasListenerFor(TOPIC_PEER_A));
        awaitTrue(() -> sourceListenerManager.hasListenerFor(TOPIC_PEER_B));

        publishConfig(TOPIC_PEER_A, "type.beta",
                new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type"),
                fullMapping());

        // Both listeners stay up after the peer-add. Process events on B to prove it never lost its consumer.
        publishSource(TOPIC_PEER_B, "b-1", "{\"type\":\"type.gamma\",\"severity\":\"info\"}");
        ConsumerRecord<String, CanonicalEvent> bCanonical = pollFirstCanonicalFor(TOPIC_PEER_B);
        assertThat(bCanonical.value().sourceEventType()).isEqualTo("type.gamma");

        assertThat(sourceListenerManager.hasListenerFor(TOPIC_PEER_A)).isTrue();
        assertThat(sourceListenerManager.hasListenerFor(TOPIC_PEER_B)).isTrue();
    }

    /* ============================== helpers ============================== */

    private static void publishConfig(String topic, String sourceEventType,
                                      ConfigDiscriminator discriminator, CanonicalMapping mapping) throws Exception {
        MappingConfig config = new MappingConfig(topic, sourceEventType, true, 2, discriminator, mapping,
                new OperationsConfig(List.of(), List.of(), List.of()));
        try (Producer<String, MappingConfig> producer = createConfigProducer()) {
            producer.send(new ProducerRecord<>(CONFIG_TOPIC,
                    new ConfigKey(topic, sourceEventType).asRecordKey(), config)).get();
            producer.flush();
        }
    }

    private static void publishTombstone(String topic, String sourceEventType) throws Exception {
        try (Producer<String, MappingConfig> producer = createConfigProducer()) {
            producer.send(new ProducerRecord<>(CONFIG_TOPIC,
                    new ConfigKey(topic, sourceEventType).asRecordKey(), null)).get();
            producer.flush();
        }
    }

    private static void publishSource(String topic, String key, String payload) throws Exception {
        try (Producer<String, byte[]> producer = createSourceProducer()) {
            producer.send(new ProducerRecord<>(topic, key, payload.getBytes(StandardCharsets.UTF_8))).get();
            producer.flush();
        }
    }

    private static ConsumerRecord<String, CanonicalEvent> pollFirstCanonicalFor(String sourceTopic) {
        try (KafkaConsumer<String, CanonicalEvent> consumer = createCanonicalConsumer()) {
            consumer.subscribe(Collections.singletonList(CANONICAL_TOPIC));
            long deadline = System.currentTimeMillis() + TIMEOUT.toMillis();
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, CanonicalEvent> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, CanonicalEvent> record : records) {
                    if (sourceTopic.equals(headerAsString(record, EventHeaders.SOURCE_TOPIC))) {
                        return record;
                    }
                }
            }
        }
        throw new AssertionError("No canonical record observed for source topic " + sourceTopic
                + " within " + TIMEOUT);
    }

    private static ConsumerRecord<String, byte[]> pollFirstDlqFor(String sourceTopic) {
        try (KafkaConsumer<String, byte[]> consumer = createDlqConsumer()) {
            consumer.subscribe(Collections.singletonList(DLQ_TOPIC));
            long deadline = System.currentTimeMillis() + TIMEOUT.toMillis();
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, byte[]> record : records) {
                    if (sourceTopic.equals(headerAsString(record, EventHeaders.SOURCE_TOPIC))) {
                        return record;
                    }
                }
            }
        }
        throw new AssertionError("No DLQ record observed for source topic " + sourceTopic
                + " within " + TIMEOUT);
    }

    private static void assertNoRecordFor(String topic, String sourceTopic) {
        Map<String, Object> props = baseConsumerProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-quiet-" + UUID.randomUUID());
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props, new StringDeserializer(), new ByteArrayDeserializer())) {
            consumer.subscribe(Collections.singletonList(topic));
            long deadline = System.currentTimeMillis() + QUIET_WINDOW.toMillis();
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, byte[]> record : records) {
                    if (sourceTopic.equals(headerAsString(record, EventHeaders.SOURCE_TOPIC))) {
                        throw new AssertionError("Unexpected record on " + topic + " for source topic " + sourceTopic);
                    }
                }
            }
        }
    }

    private static Producer<String, MappingConfig> createConfigProducer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        return new KafkaProducer<>(props, new StringSerializer(), new JsonKafkaSerializer<>());
    }

    private static Producer<String, byte[]> createSourceProducer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        return new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer());
    }

    private static KafkaConsumer<String, CanonicalEvent> createCanonicalConsumer() {
        Map<String, Object> props = baseConsumerProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-canonical-" + UUID.randomUUID());
        return new KafkaConsumer<>(props, new StringDeserializer(), new JsonKafkaDeserializer<>(CanonicalEvent.class));
    }

    private static KafkaConsumer<String, byte[]> createDlqConsumer() {
        Map<String, Object> props = baseConsumerProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-dlq-" + UUID.randomUUID());
        return new KafkaConsumer<>(props, new StringDeserializer(), new ByteArrayDeserializer());
    }

    private static Map<String, Object> baseConsumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }

    private static CanonicalMapping fullMapping() {
        return new CanonicalMapping(
                new CanonicalFieldMapping("/occurredAt"),
                new CanonicalFieldMapping("/severity"),
                new CanonicalFieldMapping("/category"),
                new CanonicalFieldMapping("/subject"),
                new CanonicalFieldMapping("/message"),
                List.of(new AttributeMapping("customerId", "/payload/customerId")));
    }

    private static void assertHeader(ConsumerRecord<?, ?> record, String name, String expectedValue) {
        Header header = record.headers().lastHeader(name);
        assertThat(header).as("expected header %s", name).isNotNull();
        assertThat(new String(header.value(), StandardCharsets.UTF_8)).isEqualTo(expectedValue);
    }

    private static String headerAsString(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static void awaitTrue(BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + TIMEOUT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while awaiting condition", e);
            }
        }
        throw new AssertionError("Condition not met within " + TIMEOUT);
    }
}
