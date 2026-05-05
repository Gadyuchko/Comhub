package io.comhub.common.config;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigCacheTests {

    private final ConfigCache cache = new ConfigCache();

    @Test
    void getReturnsNullForUnknownKey() {
        assertThat(cache.get(new ConfigKey("source.unknown", "event"))).isNull();
    }

    @Test
    void putThenGetReturnsStoredInstance() {
        ConfigKey key = new ConfigKey("source.alerts", "alert.created");
        MappingConfig config = sampleConfig("source.alerts", "alert.created");

        cache.put(key, config);

        assertThat(cache.get(key)).isSameAs(config);
    }

    @Test
    void removeDeletesEntry() {
        ConfigKey key = new ConfigKey("source.alerts", "alert.created");
        cache.put(key, sampleConfig("source.alerts", "alert.created"));

        cache.remove(key);

        assertThat(cache.get(key)).isNull();
    }

    @Test
    void sizeReflectsEntryCount() {
        assertThat(cache.size()).isZero();

        cache.put(new ConfigKey("a", "one"), sampleConfig("a", "one"));
        cache.put(new ConfigKey("b", "two"), sampleConfig("b", "two"));
        assertThat(cache.size()).isEqualTo(2);

        cache.remove(new ConfigKey("a", "one"));
        assertThat(cache.size()).isEqualTo(1);
    }

    @Test
    void configsForTopicReturnsOnlyPeerConfigs() {
        cache.put(new ConfigKey("orders.v1", "order-created"), sampleConfig("orders.v1", "order-created"));
        cache.put(new ConfigKey("orders.v1", "order-cancelled"), sampleConfig("orders.v1", "order-cancelled"));
        cache.put(new ConfigKey("alerts.v1", "alert-created"), sampleConfig("alerts.v1", "alert-created"));

        assertThat(cache.configsForTopic("orders.v1"))
                .extracting(MappingConfig::sourceEventType)
                .containsExactlyInAnyOrder("order-created", "order-cancelled");
    }

    private MappingConfig sampleConfig(String topic, String sourceEventType) {
        return new MappingConfig(
                topic,
                sourceEventType,
                true,
                2,
                new ConfigDiscriminator("header", "eventType"),
                new CanonicalMapping(null, null, null, null, null, List.of()),
                new OperationsConfig(List.of(), List.of(), List.of())
        );
    }
}
