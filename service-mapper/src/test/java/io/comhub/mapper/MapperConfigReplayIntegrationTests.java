package io.comhub.mapper;

import io.comhub.common.config.CanonicalMapping;
import io.comhub.common.config.ConfigCache;
import io.comhub.common.config.ConfigDiscriminator;
import io.comhub.common.config.DiscriminatorSource;
import io.comhub.common.config.ConfigKey;
import io.comhub.common.config.MappingConfig;
import io.comhub.common.config.OperationsConfig;
import io.comhub.common.kafka.JsonKafkaSerializer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class MapperConfigReplayIntegrationTests {

    private static final String CONFIG_TOPIC = "comhub.config.v1";

    private static EmbeddedKafkaBroker broker;
    private static ConfigurableApplicationContext context;
    private static ConfigCache cache;
    private static MeterRegistry meterRegistry;

    @BeforeAll
    static void setUp() throws Exception {
        broker = new EmbeddedKafkaKraftBroker(1, 1, CONFIG_TOPIC);
        broker.afterPropertiesSet();

        MappingConfig orderCreated = sampleConfig("orders.v1", "order-created", DiscriminatorSource.HEADER, "eventType");
        MappingConfig orderCancelled = sampleConfig("orders.v1", "order-cancelled", DiscriminatorSource.HEADER, "eventType");
        MappingConfig returned = sampleConfig("orders.v1", "order-returned", DiscriminatorSource.PAYLOAD, "/type");
        MappingConfig alertCreated = sampleConfig("alerts.v1", "alert-created", DiscriminatorSource.HEADER, "eventType");

        try (Producer<String, MappingConfig> producer = createProducer(broker.getBrokersAsString())) {
            producer.send(new ProducerRecord<>(CONFIG_TOPIC, new ConfigKey("orders.v1", "order-created").asRecordKey(), orderCreated)).get();
            producer.send(new ProducerRecord<>(CONFIG_TOPIC, new ConfigKey("orders.v1", "order-cancelled").asRecordKey(), orderCancelled)).get();
            producer.send(new ProducerRecord<>(CONFIG_TOPIC, new ConfigKey("orders.v1", "order-cancelled").asRecordKey(), null)).get();
            producer.send(new ProducerRecord<>(CONFIG_TOPIC, "malformed-key", alertCreated)).get();
            producer.send(new ProducerRecord<>(CONFIG_TOPIC, new ConfigKey("orders.v1", "order-returned").asRecordKey(), returned)).get();
            producer.send(new ProducerRecord<>(CONFIG_TOPIC, new ConfigKey("alerts.v1", "alert-created").asRecordKey(), alertCreated)).get();
            producer.flush();
        }

        SpringApplication app = new SpringApplication(MapperApplication.class);
        app.setWebApplicationType(WebApplicationType.NONE);
        context = app.run(
                "--spring.kafka.bootstrap-servers=" + broker.getBrokersAsString(),
                "--spring.application.name=service-mapper",
                "--kafka.config-replay.startup-timeout=20s",
                "--kafka.group.config-replay=test-config-replay-" + UUID.randomUUID(),
                "--kafka.topics.config-replay=" + CONFIG_TOPIC,
                "--spring.kafka.consumer.auto-offset-reset=earliest");

        cache = context.getBean(ConfigCache.class);
        meterRegistry = context.getBean(MeterRegistry.class);
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
    void replayLoadsValidMappingsIntoCompositeKeyCache() {
        assertThat(cache.get(new ConfigKey("orders.v1", "order-created"))).isNotNull();
        assertThat(cache.get(new ConfigKey("orders.v1", "order-created")).topic()).isEqualTo("orders.v1");
        assertThat(cache.get(new ConfigKey("alerts.v1", "alert-created"))).isNotNull();
    }

    @Test
    void tombstoneDuringReplayRemovesOnlyThatPeerConfig() {
        assertThat(cache.get(new ConfigKey("orders.v1", "order-cancelled"))).isNull();
        assertThat(cache.get(new ConfigKey("orders.v1", "order-created"))).isNotNull();
    }

    @Test
    void replaySkipsMalformedRecordsAndAllowsDifferentDiscriminatorsPerTopic() {
        assertThat(cache.get(new ConfigKey("orders.v1", "order-returned"))).isNotNull();
        assertThat(cache.size()).isEqualTo(3);
    }

    @Test
    void configCacheEntriesGaugeReflectsCurrentSize() {
        Gauge gauge = meterRegistry.find("comhub_config_cache_entries")
                .tag("service", "service-mapper")
                .gauge();

        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(3.0);
    }

    private static MappingConfig sampleConfig(String topic,
                                              String sourceEventType,
                                              DiscriminatorSource discriminatorSource,
                                              String discriminatorKey) {
        return new MappingConfig(
                topic,
                sourceEventType,
                true,
                2,
                new ConfigDiscriminator(discriminatorSource, discriminatorKey),
                new CanonicalMapping(null, null, null, null, null, List.of()),
                new OperationsConfig(List.of(), List.of(), List.of()));
    }

    private static Producer<String, MappingConfig> createProducer(String brokers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        return new KafkaProducer<>(props, new StringSerializer(), new JsonKafkaSerializer<>());
    }
}
