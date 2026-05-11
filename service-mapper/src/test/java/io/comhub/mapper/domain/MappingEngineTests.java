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

    @Test
    void failsOnInvalidSeverityValue() {
        MappingConfig config = sampleConfig(new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type"));
        ConsumerRecord<String, byte[]> source = sourceRecord("""
                {"type":"payment.failed","severity":"NOT_A_REAL_SEVERITY"}
                """);

        assertThatThrownBy(() -> mappingEngine.map(config, source))
                .isInstanceOf(MappingFailureException.class)
                .extracting("reason")
                .isEqualTo("severity_parse_error");
    }

    @Test
    void failsOnInvalidOccurredAt() {
        MappingConfig config = sampleConfig(new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type"));
        ConsumerRecord<String, byte[]> source = sourceRecord("""
                {"type":"payment.failed","occurredAt":"not-a-timestamp"}
                """);

        assertThatThrownBy(() -> mappingEngine.map(config, source))
                .isInstanceOf(MappingFailureException.class)
                .extracting("reason")
                .isEqualTo("occurred_at_parse_error");
    }

    @Test
    void accumulatesAllParseErrorsIntoOneException() {
        MappingConfig config = sampleConfig(new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type"));
        ConsumerRecord<String, byte[]> source = sourceRecord("""
                {"type":"payment.failed","severity":"BAD","occurredAt":"BAD"}
                """);

        assertThatThrownBy(() -> mappingEngine.map(config, source))
                .isInstanceOf(MappingFailureException.class)
                .satisfies(thrown -> {
                    String reason = ((MappingFailureException) thrown).reason();
                    assertThat(reason).contains("severity_parse_error");
                    assertThat(reason).contains("occurred_at_parse_error");
                    assertThat(reason).contains(",");
                });
    }

    @Test
    void failsWhenFieldValueIsNotTextual() {
        MappingConfig config = sampleConfig(new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type"));
        ConsumerRecord<String, byte[]> source = sourceRecord("""
                {"type":"payment.failed","severity":42}
                """);

        assertThatThrownBy(() -> mappingEngine.map(config, source))
                .isInstanceOf(MappingFailureException.class)
                .extracting("reason")
                .isEqualTo("severity_not_textual");
    }

    @Test
    void acceptsMissingOptionalFields() {
        MappingConfig config = sampleConfig(new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type"));
        ConsumerRecord<String, byte[]> source = sourceRecord("""
                {"type":"payment.failed"}
                """);

        CanonicalEvent event = mappingEngine.map(config, source);

        assertThat(event.sourceEventType()).isEqualTo("payment.failed");
        assertThat(event.occurredAt()).isNull();
        assertThat(event.severity()).isNull();
        assertThat(event.category()).isNull();
        assertThat(event.subject()).isNull();
        assertThat(event.message()).isNull();
    }

    @Test
    void failsWithDiscriminatorReasonWhenDiscriminatorPointerIsInvalid() {
        MappingConfig config = sampleConfig(new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "type"));
        ConsumerRecord<String, byte[]> source = sourceRecord("""
                {"type":"payment.failed"}
                """);

        assertThatThrownBy(() -> mappingEngine.prepare(config, source))
                .isInstanceOf(MappingFailureException.class)
                .extracting("reason")
                .isEqualTo("discriminator_extraction_failed");
    }

    @Test
    void failsWithFieldReasonWhenCanonicalPointerIsInvalid() {
        MappingConfig config = new MappingConfig(
                "orders.events.v1",
                "payment.failed",
                true,
                2,
                new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type"),
                new CanonicalMapping(
                        new CanonicalFieldMapping("/occurredAt"),
                        new CanonicalFieldMapping("severity"),
                        new CanonicalFieldMapping("/category"),
                        new CanonicalFieldMapping("/subject"),
                        new CanonicalFieldMapping("/message"),
                        List.of()),
                new OperationsConfig(List.of(), List.of(), List.of()));
        ConsumerRecord<String, byte[]> source = sourceRecord("""
                {"type":"payment.failed","severity":"error"}
                """);

        assertThatThrownBy(() -> mappingEngine.map(config, source))
                .isInstanceOf(MappingFailureException.class)
                .extracting("reason")
                .isEqualTo("severity_invalid_pointer");
    }

    @Test
    void failsWithAttributeReasonWhenAttributePointerIsInvalid() {
        MappingConfig config = new MappingConfig(
                "orders.events.v1",
                "payment.failed",
                true,
                2,
                new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type"),
                new CanonicalMapping(
                        new CanonicalFieldMapping("/occurredAt"),
                        new CanonicalFieldMapping("/severity"),
                        new CanonicalFieldMapping("/category"),
                        new CanonicalFieldMapping("/subject"),
                        new CanonicalFieldMapping("/message"),
                        List.of(new AttributeMapping("customerId", "payload/customerId"))),
                new OperationsConfig(List.of(), List.of(), List.of()));
        ConsumerRecord<String, byte[]> source = sourceRecord("""
                {"type":"payment.failed","payload":{"customerId":"cust-123"}}
                """);

        assertThatThrownBy(() -> mappingEngine.map(config, source))
                .isInstanceOf(MappingFailureException.class)
                .extracting("reason")
                .isEqualTo("attribute_invalid_pointer");
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
