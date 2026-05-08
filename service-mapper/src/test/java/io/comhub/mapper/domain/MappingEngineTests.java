package io.comhub.mapper.domain;

import io.comhub.common.config.AttributeMapping;
import io.comhub.common.config.CanonicalFieldMapping;
import io.comhub.common.config.CanonicalMapping;
import io.comhub.common.config.ConfigDiscriminator;
import io.comhub.common.config.DiscriminatorSource;
import io.comhub.common.config.MappingConfig;
import io.comhub.common.config.OperationsConfig;
import io.comhub.common.event.CanonicalEvent;
import io.comhub.common.event.Severity;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MappingEngineTests {

    private final MappingEngine mappingEngine = new MappingEngine();

    @Test
    void mapsPayloadDiscriminatorAndCanonicalFields() {
        MappingConfig config = sampleConfig(new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type"));
        ConsumerRecord<String, byte[]> source = sourceRecord("""
                {"type":"payment.failed","occurredAt":"2026-05-08T09:00:00Z","severity":"error","category":"payments","subject":"Payment failed","message":"Card declined","payload":{"customerId":"cust-123"}}
                """);

        CanonicalEvent event = mappingEngine.map(config, source);

        assertThat(event.sourceTopic()).isEqualTo("orders.events.v1");
        assertThat(event.sourceEventType()).isEqualTo("payment.failed");
        assertThat(event.sourcePartition()).isEqualTo(2);
        assertThat(event.sourceOffset()).isEqualTo(42L);
        assertThat(event.occurredAt()).isEqualTo(Instant.parse("2026-05-08T09:00:00Z"));
        assertThat(event.severity()).isEqualTo(Severity.ERROR);
        assertThat(event.category()).isEqualTo("payments");
        assertThat(event.subject()).isEqualTo("Payment failed");
        assertThat(event.message()).isEqualTo("Card declined");
        assertThat(event.attributes()).containsEntry("customerId", "cust-123");
        assertThat(event.rawPayload()).contains("\"type\":\"payment.failed\"");
        assertThat(event.classificationCode()).isNull();
        assertThat(event.handler()).isNull();
    }

    @Test
    void mapsHeaderDiscriminator() {
        MappingConfig config = sampleConfig(new ConfigDiscriminator(DiscriminatorSource.HEADER, "eventType"));
        ConsumerRecord<String, byte[]> source = sourceRecord("{\"severity\":\"warning\"}");
        source.headers().add(new RecordHeader("eventType", "payment.failed".getBytes(StandardCharsets.UTF_8)));

        PreparedSourceEvent prepared = mappingEngine.prepare(config, source);
        CanonicalEvent event = mappingEngine.map(config, prepared);

        assertThat(prepared.sourceEventType()).isEqualTo("payment.failed");
        assertThat(event.sourceEventType()).isEqualTo("payment.failed");
        assertThat(event.severity()).isEqualTo(Severity.WARNING);
    }

    @Test
    void mapsTopicDiscriminator() {
        MappingConfig config = sampleConfig("orders.events.v1", new ConfigDiscriminator(DiscriminatorSource.TOPIC, null));
        ConsumerRecord<String, byte[]> source = sourceRecord("{\"severity\":\"info\"}");

        PreparedSourceEvent prepared = mappingEngine.prepare(config, source);
        CanonicalEvent event = mappingEngine.map(config, prepared);

        assertThat(prepared.sourceEventType()).isEqualTo("orders.events.v1");
        assertThat(event.sourceEventType()).isEqualTo("orders.events.v1");
        assertThat(event.severity()).isEqualTo(Severity.INFO);
    }

    @Test
    void failsWhenDiscriminatorCannotBeExtracted() {
        MappingConfig config = sampleConfig(new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type"));
        ConsumerRecord<String, byte[]> source = sourceRecord("{\"severity\":\"error\"}");

        assertThatThrownBy(() -> mappingEngine.map(config, source))
                .isInstanceOf(MappingFailureException.class)
                .extracting("reason")
                .isEqualTo("discriminator_extraction_failed");
    }

    @Test
    void failsWhenPayloadIsNotJson() {
        MappingConfig config = sampleConfig(new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type"));
        ConsumerRecord<String, byte[]> source = sourceRecord("not-json");

        assertThatThrownBy(() -> mappingEngine.map(config, source))
                .isInstanceOf(MappingFailureException.class)
                .extracting("reason")
                .isEqualTo("payload_parse_error");
    }

    private MappingConfig sampleConfig(ConfigDiscriminator discriminator) {
        return sampleConfig("payment.failed", discriminator);
    }

    private MappingConfig sampleConfig(String sourceEventType, ConfigDiscriminator discriminator) {
        return new MappingConfig(
                "orders.events.v1",
                sourceEventType,
                true,
                2,
                discriminator,
                new CanonicalMapping(
                        new CanonicalFieldMapping("/occurredAt"),
                        new CanonicalFieldMapping("/severity"),
                        new CanonicalFieldMapping("/category"),
                        new CanonicalFieldMapping("/subject"),
                        new CanonicalFieldMapping("/message"),
                        List.of(new AttributeMapping("customerId", "/payload/customerId"))),
                new OperationsConfig(List.of(), List.of(), List.of()));
    }

    private ConsumerRecord<String, byte[]> sourceRecord(String payload) {
        return new ConsumerRecord<>(
                "orders.events.v1",
                2,
                42L,
                "order-1",
                payload.getBytes(StandardCharsets.UTF_8));
    }
}
