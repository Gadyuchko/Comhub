package io.comhub.common.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigKeyTests {

    @Test
    void formatAndParseRoundTrip() {
        ConfigKey key = new ConfigKey("orders.v1", "order-created");

        assertThat(ConfigKey.parse(key.asRecordKey())).isEqualTo(key);
    }

    @Test
    void parseRejectsMalformedCompositeKey() {
        assertThat(ConfigKey.parse("orders.v1")).isNull();
        assertThat(ConfigKey.parse("orders.v1::")).isNull();
        assertThat(ConfigKey.parse("::order-created")).isNull();
        assertThat(ConfigKey.parse("orders::v1::order-created")).isNull();
    }
}
