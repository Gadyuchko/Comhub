package io.comhub.common.logging;

import io.comhub.common.event.CanonicalEvent;
import org.slf4j.MDC;

import java.util.Map;

/**
 * Restricts MDC population to low-cardinality Comhub operational fields.
 *
 * @author Roman Hadiuchko
 */
public final class LoggingContext {

    public static final String EVENT_ID = "event.id";
    public static final String SERVICE = "service";
    public static final String STAGE = "stage";
    public static final String RESULT = "result";
    public static final String ATTEMPT = "attempt";
    public static final String SOURCE_TOPIC = "sourceTopic";
    public static final String CATEGORY = "category";
    public static final String SEVERITY = "severity";

    public static final Map<String, String> ALLOWED_KEYS = Map.of(
            EVENT_ID, EVENT_ID,
            SERVICE, SERVICE,
            STAGE, STAGE,
            RESULT, RESULT,
            ATTEMPT, ATTEMPT,
            SOURCE_TOPIC, SOURCE_TOPIC,
            CATEGORY, CATEGORY,
            SEVERITY, SEVERITY
    );

    private LoggingContext() {
        throw new UnsupportedOperationException("LoggingContext contains static MDC helpers only");
    }

    public static void putService(String service) {
        putIfPresent(SERVICE, service);
    }

    public static void putStage(String stage) {
        putIfPresent(STAGE, stage);
    }

    public static void putResult(String result) {
        putIfPresent(RESULT, result);
    }

    public static void putAttempt(Integer attempt) {
        if (attempt != null) {
            MDC.put(ATTEMPT, attempt.toString());
        }
    }

    public static void putEvent(CanonicalEvent event) {
        if (event == null) {
            return;
        }

        putIfPresent(EVENT_ID, event.id() == null ? null : event.id().toString());
        putIfPresent(SOURCE_TOPIC, event.sourceTopic());
        putIfPresent(CATEGORY, event.category());
        putIfPresent(SEVERITY, event.severity() == null ? null : event.severity().name());
    }

    public static void clear() {
        for (String key : ALLOWED_KEYS.keySet()) {
            MDC.remove(key);
        }
    }

    private static void putIfPresent(String key, String value) {
        if (value != null && !value.isBlank()) {
            MDC.put(key, value);
        }
    }
}
