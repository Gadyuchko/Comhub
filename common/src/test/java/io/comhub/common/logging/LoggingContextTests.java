package io.comhub.common.logging;

import io.comhub.common.event.CanonicalEvent;
import io.comhub.common.event.Severity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class LoggingContextTests {

    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    @Test
    void setsOnlyAllowedMdcKeys() {
        LoggingContext.putService("mapper");
        LoggingContext.putStage("mapping");
        LoggingContext.putResult("success");
        LoggingContext.putAttempt(2);
        LoggingContext.putEvent(sampleEvent());

        Map<String, String> copy = MDC.getCopyOfContextMap();

        assertThat(copy).containsOnlyKeys(
                "event.id",
                "service",
                "stage",
                "result",
                "attempt",
                "sourceTopic",
                "category",
                "severity"
        );
        assertThat(copy).containsEntry("sourceTopic", "source.ops");
        assertThat(copy).containsEntry("severity", "CRITICAL");
    }

    @Test
    void publicApiDoesNotAcceptPayloadLikeDataOrFreeFormFields() {
        assertThat(Arrays.stream(LoggingContext.class.getMethods())
                .filter(method -> method.getDeclaringClass().equals(LoggingContext.class))
                .map(Method::getName))
                .noneMatch(name -> name.toLowerCase().contains("payload"))
                .noneMatch(name -> name.toLowerCase().contains("secret"))
                .noneMatch(name -> name.toLowerCase().contains("recipient"))
                .noneMatch(name -> name.equals("put"));

        assertThat(LoggingContext.ALLOWED_KEYS.keySet()).containsExactlyInAnyOrder(
                "event.id",
                "service",
                "stage",
                "result",
                "attempt",
                "sourceTopic",
                "category",
                "severity"
        );
    }

    @Test
    void clearRemovesAllowedContextFields() {
        LoggingContext.putService("router");
        LoggingContext.putStage("classification");

        LoggingContext.clear();

        assertThat(MDC.getCopyOfContextMap()).isNullOrEmpty();
    }

    private CanonicalEvent sampleEvent() {
        return new CanonicalEvent(
                UUID.fromString("00000000-0000-0000-0000-000000000333"),
                "source.ops",
                1,
                99L,
                Instant.parse("2026-04-15T10:00:00Z"),
                Instant.parse("2026-04-15T09:57:00Z"),
                Severity.CRITICAL,
                "ops",
                "broker",
                "Broker unavailable",
                Map.of("ignored", "not logged"),
                "{\"ignored\":\"not logged\"}"
        );
    }
}
