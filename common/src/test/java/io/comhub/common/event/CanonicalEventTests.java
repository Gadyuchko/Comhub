package io.comhub.common.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class CanonicalEventTests {

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    void jacksonRoundTripPreservesAllFields() throws Exception {
        CanonicalEvent event = sampleEvent();

        String json = objectMapper.writeValueAsString(event);
        CanonicalEvent restored = objectMapper.readValue(json, CanonicalEvent.class);

        assertThat(restored).isEqualTo(event);
        assertThat(restored.attributes()).containsEntry("host", "edge-1");
    }

    @Test
    void hasExpectedSeverityValuesInStableOrder() {
        assertThat(Severity.values())
                .extracting(Enum::name)
                .containsExactly("INFO", "WARNING", "ERROR", "CRITICAL");
    }

    @Test
    void eventHeaderConstantsMatchContract() {
        assertThat(EventHeaders.EVENT_ID).isEqualTo("comhub.event.id");
        assertThat(EventHeaders.SOURCE_TOPIC).isEqualTo("comhub.source.topic");
        assertThat(EventHeaders.SOURCE_PARTITION).isEqualTo("comhub.source.partition");
        assertThat(EventHeaders.SOURCE_OFFSET).isEqualTo("comhub.source.offset");
        assertThat(EventHeaders.SOURCE_EVENT_TYPE).isEqualTo("comhub.source.event.type");
        assertThat(EventHeaders.ATTEMPT_COUNT).isEqualTo("comhub.attempt.count");
        assertThat(EventHeaders.FAILURE_STAGE).isEqualTo("comhub.failure.stage");
        assertThat(EventHeaders.FAILURE_REASON).isEqualTo("comhub.failure.reason");
        assertThat(EventHeaders.TRACEPARENT).isEqualTo("traceparent");
    }

    private CanonicalEvent sampleEvent() {
        return new CanonicalEvent(
                UUID.fromString("00000000-0000-0000-0000-000000000111"),
                "source.alerts",
                "alert.created",
                2,
                42L,
                Instant.parse("2026-04-15T10:00:00Z"),
                Instant.parse("2026-04-15T09:59:00Z"),
                Severity.WARNING,
                "infra",
                "INFRA_WARN",
                "ops-default",
                "disk",
                "Disk pressure",
                Map.of("host", "edge-1"),
                "{\"host\":\"edge-1\"}"
        );
    }
}
