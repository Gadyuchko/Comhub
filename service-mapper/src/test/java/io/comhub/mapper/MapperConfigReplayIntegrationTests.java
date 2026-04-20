package io.comhub.mapper;

import io.comhub.common.config.ConfigCache;
import io.comhub.common.config.MappingConfig;
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

/**
 * Verifies that the mapper service correctly materializes {@code comhub.config.v1}
 * into {@link ConfigCache} at startup and honors tombstone semantics during replay.
 *
 * <p>Boots an embedded Kafka broker, seeds the config topic with a mix of upserts
 * and a tombstone <em>before</em> starting the Spring context, then asserts the
 * post-startup cache contents. This is the only way to observe replay end-of-topic
 * behavior: once the Spring context has started, the coordinator's latch has
 * already released, so the cache state at that moment is exactly what replay
 * produced.
 *
 * @author Roman Hadiuchko
 */
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

        MappingConfig m1 = new MappingConfig("source.one",   "Source One",   true, 1, List.of(), "one@example.com");
        MappingConfig m2 = new MappingConfig("source.two",   "Source Two",   true, 1, List.of(), "two@example.com");
        MappingConfig m3 = new MappingConfig("source.three", "Source Three", true, 1, List.of(), "three@example.com");

        try (Producer<String, MappingConfig> producer = createProducer(broker.getBrokersAsString())) {
            producer.send(new ProducerRecord<>(CONFIG_TOPIC, "m1", m1)).get();
            producer.send(new ProducerRecord<>(CONFIG_TOPIC, "m2", m2)).get();
            producer.send(new ProducerRecord<>(CONFIG_TOPIC, "m3", m3)).get();
            // Tombstone for m3 — replay should observe this and remove the entry.
            producer.send(new ProducerRecord<>(CONFIG_TOPIC, "m3", null)).get();
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
    void replayLoadsNonTombstonedMappings() {
        assertThat(cache.get("m1")).isNotNull();
        assertThat(cache.get("m1").sourceTopic()).isEqualTo("source.one");

        assertThat(cache.get("m2")).isNotNull();
        assertThat(cache.get("m2").sourceTopic()).isEqualTo("source.two");
    }

    @Test
    void tombstoneDuringReplayRemovesEntry() {
        assertThat(cache.get("m3")).isNull();
    }

    @Test
    void cacheSizeMatchesPostReplayState() {
        assertThat(cache.size()).isEqualTo(2);
    }

    @Test
    void configCacheEntriesGaugeReflectsCurrentSize() {
        Gauge gauge = meterRegistry.find("comhub_config_cache_entries")
                .tag("service", "service-mapper")
                .gauge();

        assertThat(gauge).as("gauge registered").isNotNull();
        assertThat(gauge.value()).isEqualTo(2.0);
    }

    private static Producer<String, MappingConfig> createProducer(String brokers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        return new KafkaProducer<>(props, new StringSerializer(), new JsonKafkaSerializer<>());
    }
}
