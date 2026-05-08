package io.comhub.mapper.domain;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Holds source record details after mapper parsed payload and extracted source event type.
 *
 * @author Roman Hadiuchko
 */
public record PreparedSourceEvent(
        ConsumerRecord<String, byte[]> source,
        JsonNode payload,
        String sourceEventType) {
}
