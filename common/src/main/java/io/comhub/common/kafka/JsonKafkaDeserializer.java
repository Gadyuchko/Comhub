package io.comhub.common.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

/**
 * Jackson-backed Kafka deserializer for JSON topic values without Schema Registry.
 *
 * @author Roman Hadiuchko
 */
public final class JsonKafkaDeserializer<T> implements Deserializer<T> {

    public static final String TARGET_TYPE_CONFIG = "comhub.json.target.type";

    private final ObjectMapper objectMapper;
    private Class<T> targetType;

    public JsonKafkaDeserializer() {
        this(null);
    }

    public JsonKafkaDeserializer(Class<T> targetType) {
        this.objectMapper = JacksonSupport.objectMapper();
        this.targetType = targetType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs, boolean isKey) {
        Object configuredType = configs.get(TARGET_TYPE_CONFIG);
        if (configuredType instanceof Class<?> clazz) {
            this.targetType = (Class<T>) clazz;
            return;
        }

        if (configuredType instanceof String className) {
            this.targetType = loadClass(className);
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        if (targetType == null) {
            throw new SerializationException("Target type must be configured before deserialization");
        }

        try {
            return objectMapper.readValue(data, targetType);
        } catch (IOException ex) {
            throw new SerializationException("Failed to deserialize JSON from topic " + topic, ex);
        }
    }

    @SuppressWarnings("unchecked")
    private Class<T> loadClass(String className) {
        try {
            return (Class<T>) Class.forName(className);
        } catch (ClassNotFoundException ex) {
            throw new SerializationException("Configured target type was not found: " + className, ex);
        }
    }
}
