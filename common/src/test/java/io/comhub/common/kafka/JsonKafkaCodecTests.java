package io.comhub.common.kafka;

import io.comhub.common.event.CanonicalEvent;
import io.comhub.common.event.Severity;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JsonKafkaCodecTests {

    @Test
    void serializerAndDeserializerRoundTripCanonicalEvent() {
        CanonicalEvent event = sampleEvent();

        try (JsonKafkaSerializer<CanonicalEvent> serializer = new JsonKafkaSerializer<>();
             JsonKafkaDeserializer<CanonicalEvent> deserializer = new JsonKafkaDeserializer<>(CanonicalEvent.class)) {
            byte[] bytes = serializer.serialize("canonical.events", event);
            CanonicalEvent restored = deserializer.deserialize("canonical.events", bytes);

            assertThat(restored).isEqualTo(event);
        }
    }

    @Test
    void deserializerCanUseConfiguredTargetType() {
        CanonicalEvent event = sampleEvent();

        try (JsonKafkaSerializer<CanonicalEvent> serializer = new JsonKafkaSerializer<>();
             JsonKafkaDeserializer<CanonicalEvent> deserializer = new JsonKafkaDeserializer<>()) {
            byte[] bytes = serializer.serialize("canonical.events", event);

            deserializer.configure(Map.of(JsonKafkaDeserializer.TARGET_TYPE_CONFIG, CanonicalEvent.class), false);

            assertThat(deserializer.deserialize("canonical.events", bytes)).isEqualTo(event);
        }
    }

    @Test
    void invalidJsonSurfacesAsSerializationFailure() {
        try (JsonKafkaDeserializer<CanonicalEvent> deserializer = new JsonKafkaDeserializer<>(CanonicalEvent.class)) {
            assertThatThrownBy(() -> deserializer.deserialize("canonical.events", "{bad json".getBytes(StandardCharsets.UTF_8)))
                    .isInstanceOf(SerializationException.class)
                    .hasMessageContaining("Failed to deserialize JSON");
        }

    }

    private CanonicalEvent sampleEvent() {
        return new CanonicalEvent(
                UUID.fromString("00000000-0000-0000-0000-000000000222"),
                "source.ops",
                0,
                8L,
                Instant.parse("2026-04-15T10:00:00Z"),
                Instant.parse("2026-04-15T09:58:00Z"),
                Severity.ERROR,
                "ops",
                "router",
                "Routing failed",
                Map.of("region", "local"),
                "{\"region\":\"local\"}"
        );
    }
}
