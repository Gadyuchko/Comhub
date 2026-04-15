package io.comhub.common.event;

/**
 * Canonical Kafka header names used to preserve Comhub event traceability.
 *
 * @author Roman Hadiuchko
 */
public final class EventHeaders {

    public static final String EVENT_ID = "comhub.event.id";
    public static final String SOURCE_TOPIC = "comhub.source.topic";
    public static final String SOURCE_PARTITION = "comhub.source.partition";
    public static final String SOURCE_OFFSET = "comhub.source.offset";
    public static final String ATTEMPT_COUNT = "comhub.attempt.count";
    public static final String TRACEPARENT = "traceparent";

    private EventHeaders() {
        throw new UnsupportedOperationException("EventHeaders contains constants only");
    }
}
