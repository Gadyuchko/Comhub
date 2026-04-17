package io.comhub.common.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.comhub.common.json.JacksonSupport;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MappingConfigTests {

    private final ObjectMapper objectMapper = JacksonSupport.sharedObjectMapper();

    @Test
    void jacksonRoundTripPreservesAllFields() throws Exception {
        MappingConfig original = new MappingConfig(
                "source.alerts",
                "Alerts source",
                true,
                2,
                List.of(new MappingRule()),
                "ops@example.com"
        );

        String json = objectMapper.writeValueAsString(original);
        MappingConfig restored = objectMapper.readValue(json, MappingConfig.class);

        assertThat(restored).isEqualTo(original);
    }

    @Test
    void emptyJsonAppliesCompactConstructorDefaults() throws Exception {
        MappingConfig restored = objectMapper.readValue("{}", MappingConfig.class);

        assertThat(restored.configSchemaVersion()).isEqualTo(1);
        assertThat(restored.rules()).isEmpty();
    }

    @Test
    void explicitSchemaVersionIsPreserved() throws Exception {
        MappingConfig restored = objectMapper.readValue(
                "{\"configSchemaVersion\":7}", MappingConfig.class);

        assertThat(restored.configSchemaVersion()).isEqualTo(7);
    }

    @Test
    void nullRulesArgumentBecomesEmptyList() {
        MappingConfig config = new MappingConfig(
                "source.alerts", "Alerts", true, 1, null, "ops@example.com");

        assertThat(config.rules()).isEmpty();
    }

    @Test
    void rulesListIsImmutableAfterConstruction() {
        List<MappingRule> source = new ArrayList<>(List.of(new MappingRule()));
        MappingConfig config = new MappingConfig(
                "source.alerts", "Alerts", true, 1, source, "ops@example.com");

        assertThatThrownBy(() -> config.rules().add(new MappingRule()))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
