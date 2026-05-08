package io.comhub.mapper.domain;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import io.comhub.common.config.AttributeMapping;
import io.comhub.common.config.CanonicalFieldMapping;
import io.comhub.common.config.CanonicalMapping;
import io.comhub.common.config.ConfigDiscriminator;
import io.comhub.common.config.MappingConfig;
import io.comhub.common.event.CanonicalEvent;
import io.comhub.common.event.Severity;
import io.comhub.common.json.JacksonSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * Maps raw source-event JSON into the shared canonical event envelope.
 *
 * <p>Mapper does not know source payload shapes ahead of time. The config tells it where to look.
 * It holds a {@link CanonicalMapping} that has exact mappings from source payload fields to
 * canonical fields. Also, because source event type can come from either headers or payload,
 * config holds a {@link ConfigDiscriminator} that tells mapper where to look for the source
 * event type value.
 *
 * <p>This class only transforms one already-consumed Kafka record. It does not read from Kafka,
 * write to Kafka, acknowledge offsets, or decide whether a failed record goes to DLQ. When the
 * record cannot be mapped, it throws {@link MappingFailureException} with a reason the listener
 * can later put into DLQ headers.
 *
 * @author Roman Hadiuchko
 */
@Component
public class MappingEngine {

    /**
     * Turns one raw source Kafka record into a canonical event.
     *
     * <p>First it parses source bytes into JSON. Then it reads the real source event type using
     * the discriminator from config. After that it reads configured canonical fields from the
     * same JSON payload. Kafka topic, partition, and offset are copied from the consumed record
     * so we can always trace where the canonical event came from.
     *
     * <p>Classification and handler stay {@code null} here. Mapper only transforms the event;
     * router will decide classification and handler later.
     */
    public CanonicalEvent map(MappingConfig config, ConsumerRecord<String, byte[]> source) {
        return map(config, prepare(config, source));
    }

    public CanonicalEvent map(MappingConfig config, PreparedSourceEvent prepared) {
        ConsumerRecord<String, byte[]> source = prepared.source();
        JsonNode payload = prepared.payload();
        String sourceEventType = prepared.sourceEventType();

        // Config mapping is used for fields that come from the source JSON.
        // Kafka record metadata is used later for source topic, partition, and offset.
        CanonicalMapping mapping = config.mapping();

        return new CanonicalEvent(
                UUID.randomUUID(),
                source.topic(),
                sourceEventType,
                source.partition(),
                source.offset(),
                Instant.now(),
                parseInstant(resolveText(mapping == null ? null : mapping.occurredAt(), payload)),
                parseSeverity(resolveText(mapping == null ? null : mapping.severity(), payload)),
                resolveText(mapping == null ? null : mapping.category(), payload),
                null,
                null,
                resolveText(mapping == null ? null : mapping.subject(), payload),
                resolveText(mapping == null ? null : mapping.message(), payload),
                resolveAttributes(mapping, payload),
                rawPayload(source.value())
        );
    }

    private JsonNode parsePayload(byte[] value) {
        if (value == null) {
            return NullNode.getInstance();
        }

        try {
            return JacksonSupport.sharedObjectMapper().readTree(value);
        } catch (IOException e) {
            throw new MappingFailureException("payload_parse_error", e);
        }
    }

    public PreparedSourceEvent prepare(MappingConfig config, ConsumerRecord<String, byte[]> source) {
        JsonNode payload = parsePayload(source.value());
        String sourceEventType = extractSourceEventType(config.discriminator(), payload, source);

        return new PreparedSourceEvent(source, payload, sourceEventType);
    }

    /**
     * Retrieves source event type based on discrimination source and key.
     * If source is header it will look in headers. If source is payload it will look in payload.
     */
    private String extractSourceEventType(ConfigDiscriminator discriminator,
                                          JsonNode payload,
                                          ConsumerRecord<String, byte[]> source) {
        if (discriminator == null || discriminator.source() == null) {
            throw new MappingFailureException("discriminator_extraction_failed");
        }

        return switch (discriminator.source()) {
            case TOPIC -> source.topic();
            case HEADER -> extractHeaderSourceEventType(discriminator, source);
            case PAYLOAD -> extractPayloadSourceEventType(discriminator, payload);
        };
    }

    private String extractHeaderSourceEventType(ConfigDiscriminator discriminator,
                                                ConsumerRecord<String, byte[]> source) {
        if (isBlank(discriminator.key())) {
            throw new MappingFailureException("discriminator_extraction_failed");
        }

        Header header = source.headers().lastHeader(discriminator.key());
        if (header == null || header.value() == null) {
            throw new MappingFailureException("discriminator_extraction_failed");
        }

        String value = new String(header.value(), StandardCharsets.UTF_8);
        if (isBlank(value)) {
            throw new MappingFailureException("discriminator_extraction_failed");
        }
        return value;
    }

    private String extractPayloadSourceEventType(ConfigDiscriminator discriminator, JsonNode payload) {
        if (isBlank(discriminator.key())) {
            throw new MappingFailureException("discriminator_extraction_failed");
        }

        String value = resolveRequiredText(discriminator.key(), payload);
        if (isBlank(value)) {
            throw new MappingFailureException("discriminator_extraction_failed");
        }
        return value;
    }

    private String resolveRequiredText(String pointer, JsonNode payload) {
        JsonNode value = payload.at(JsonPointer.compile(pointer));
        if (value.isMissingNode() || value.isNull() || !value.isTextual()) {
            throw new MappingFailureException("discriminator_extraction_failed");
        }
        return value.asText();
    }

    private String resolveText(CanonicalFieldMapping fieldMapping, JsonNode payload) {
        if (fieldMapping == null || isBlank(fieldMapping.source())) {
            return null;
        }

        JsonNode value = payload.at(JsonPointer.compile(fieldMapping.source()));
        if (value.isMissingNode() || value.isNull()) {
            return null;
        }
        return value.isTextual() ? value.asText() : value.asText(null);
    }

    private Map<String, String> resolveAttributes(CanonicalMapping mapping, JsonNode payload) {
        if (mapping == null) {
            return Map.of();
        }

        Map<String, String> attributes = new LinkedHashMap<>();
        for (AttributeMapping attribute : mapping.attributes()) {
            if (attribute == null || isBlank(attribute.targetAttribute()) || isBlank(attribute.source())) {
                continue;
            }

            JsonNode value = payload.at(JsonPointer.compile(attribute.source()));
            if (!value.isMissingNode() && !value.isNull() && value.isTextual() && !isBlank(value.asText())) {
                attributes.put(attribute.targetAttribute(), value.asText());
            }
        }
        return attributes;
    }

    private Severity parseSeverity(String value) {
        if (isBlank(value)) {
            return null;
        }

        try {
            return Severity.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private Instant parseInstant(String value) {
        if (isBlank(value)) {
            return null;
        }

        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    private String rawPayload(byte[] value) {
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
