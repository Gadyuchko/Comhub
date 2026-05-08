package io.comhub.common.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.comhub.common.json.JacksonSupport;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MappingConfigTests {

    private final ObjectMapper objectMapper = JacksonSupport.sharedObjectMapper();

    @Test
    void jacksonRoundTripPreservesAllFields() throws Exception {
        MappingConfig original = sampleConfig();

        String json = objectMapper.writeValueAsString(original);
        MappingConfig restored = objectMapper.readValue(json, MappingConfig.class);

        assertThat(restored).isEqualTo(original);
    }

    @Test
    void emptyJsonAppliesCompactConstructorDefaults() throws Exception {
        MappingConfig restored = objectMapper.readValue("{}", MappingConfig.class);

        assertThat(restored.configSchemaVersion()).isEqualTo(2);
    }

    @Test
    void explicitSchemaVersionIsPreserved() throws Exception {
        MappingConfig restored = objectMapper.readValue(
                "{\"configSchemaVersion\":7}", MappingConfig.class);

        assertThat(restored.configSchemaVersion()).isEqualTo(7);
    }

    @Test
    void nullNestedCollectionsBecomeImmutableEmptyCollections() {
        MappingConfig config = new MappingConfig(
                "source.alerts",
                "alert.created",
                true,
                0,
                new ConfigDiscriminator(DiscriminatorSource.HEADER, "eventType"),
                new CanonicalMapping(null, null, null, null, null, null),
                new OperationsConfig(null, null, null));

        assertThat(config.mapping().attributes()).isEmpty();
        assertThat(config.operations().promotedAttributes()).isEmpty();
        assertThat(config.operations().classification()).isEmpty();
        assertThat(config.operations().routing()).isEmpty();
    }

    @Test
    void nestedCollectionsAreImmutableAfterConstruction() {
        MappingConfig config = sampleConfig();

        assertThatThrownBy(() -> config.mapping().attributes().add(new AttributeMapping("team", "/team")))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> config.operations().promotedAttributes().add(new PromotedAttribute("team", "team")))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> config.operations().classification().getFirst().conditions().add(new Condition("a", "eq", "b")))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> config.operations().routing().getFirst().actions().add(new RoutingAction("notify", "email", "x@example.com")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private MappingConfig sampleConfig() {
        return new MappingConfig(
                "source.alerts",
                "alert.created",
                true,
                2,
                new ConfigDiscriminator(DiscriminatorSource.HEADER, "eventType"),
                new CanonicalMapping(
                        new CanonicalFieldMapping("/occurredAt"),
                        new CanonicalFieldMapping("/severity"),
                        new CanonicalFieldMapping("/category"),
                        new CanonicalFieldMapping("/subject"),
                        new CanonicalFieldMapping("/message"),
                        List.of(new AttributeMapping("host", "/host"))),
                new OperationsConfig(
                        List.of(new PromotedAttribute("host", "host")),
                        List.of(new ClassificationRule(
                                "OPS",
                                "ops-default",
                                List.of(new Condition("severity", "eq", "CRITICAL")))),
                        List.of(new RoutingRule(
                                "primary-email",
                                List.of(new Condition("classificationCode", "eq", "OPS")),
                                List.of(new RoutingAction("notify", "email", "ops@example.com"))))));
    }
}
