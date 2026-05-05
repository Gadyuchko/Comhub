package io.comhub.common.config;

/**
 * Composite identity of one source-event configuration entry.
 *
 * @author Roman Hadiuchko
 */
public record ConfigKey(
        String topic,
        String sourceEventType) {

    public static final String SEPARATOR = "::";

    public String asRecordKey() {
        return topic + SEPARATOR + sourceEventType;
    }

    public static ConfigKey parse(String recordKey) {
        if (recordKey == null || recordKey.isBlank()) {
            return null;
        }

        int separatorIndex = recordKey.indexOf(SEPARATOR);
        if (separatorIndex <= 0 || separatorIndex != recordKey.lastIndexOf(SEPARATOR)) {
            return null;
        }

        String topic = recordKey.substring(0, separatorIndex);
        String sourceEventType = recordKey.substring(separatorIndex + SEPARATOR.length());
        if (topic.isBlank() || sourceEventType.isBlank()) {
            return null;
        }

        return new ConfigKey(topic, sourceEventType);
    }
}
