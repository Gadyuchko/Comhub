package io.comhub.common.event;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Immutable v1 canonical event envelope shared across Comhub services.
 *
 * @author Roman Hadiuchko
 */
public record CanonicalEvent(
        UUID id,
        String sourceTopic,
        String sourceEventType,
        Integer sourcePartition,
        Long sourceOffset,
        Instant receivedAt,
        Instant occurredAt,
        Severity severity,
        String category,
        String classificationCode,
        String handler,
        String subject,
        String message,
        Map<String, String> attributes,
        String rawPayload
) {

    /**
     * Copies attributes defensively so the event remains immutable after construction.
     */
    public CanonicalEvent {
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }
}
