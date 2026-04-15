package io.comhub.common.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Jackson-backed Kafka serializer for JSON topic values without Schema Registry.
 *
 * @author Roman Hadiuchko
 */
public final class JsonKafkaSerializer<T> implements Serializer<T> {

    private final ObjectMapper objectMapper = JacksonSupport.objectMapper();

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }

        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException ex) {
            throw new SerializationException("Failed to serialize JSON for topic " + topic, ex);
        }
    }
}
