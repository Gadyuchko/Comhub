package io.comhub.common.config;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigCacheTests {

    private final ConfigCache cache = new ConfigCache();

    @Test
    void getReturnsNullForUnknownKey() {
        assertThat(cache.get("source.unknown")).isNull();
    }

    @Test
    void putThenGetReturnsStoredInstance() {
        MappingConfig config = sampleConfig("source.alerts");

        cache.put("source.alerts", config);

        assertThat(cache.get("source.alerts")).isSameAs(config);
    }

    @Test
    void removeDeletesEntry() {
        cache.put("source.alerts", sampleConfig("source.alerts"));

        cache.remove("source.alerts");

        assertThat(cache.get("source.alerts")).isNull();
    }

    @Test
    void sizeReflectsEntryCount() {
        assertThat(cache.size()).isZero();

        cache.put("a", sampleConfig("a"));
        cache.put("b", sampleConfig("b"));
        assertThat(cache.size()).isEqualTo(2);

        cache.remove("a");
        assertThat(cache.size()).isEqualTo(1);
    }

    private MappingConfig sampleConfig(String sourceTopic) {
        return new MappingConfig(
                sourceTopic,
                "Display " + sourceTopic,
                true,
                1,
                List.of(),
                "ops@example.com"
        );
    }
}
