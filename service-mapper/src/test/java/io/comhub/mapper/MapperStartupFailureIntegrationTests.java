package io.comhub.mapper;

import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.SpringApplication;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that when the config topic cannot be consumed at startup, the
 * service fails fast rather than continuing with an empty cache.
 *
 * <p>Matches the Kafka-unavailable acceptance criterion: no snapshot fallback,
 * failure surfaced through normal Spring Boot startup with a visible exception.
 * Achieves the unreachable-broker condition by pointing the listener at a port
 * on localhost where nothing listens, combined with a tight startup timeout so
 * the test finishes in seconds rather than blocking on TCP retries.
 *
 * @author Roman Hadiuchko
 */
class MapperStartupFailureIntegrationTests {

    @Test
    void startupFailsWhenConfigReplayTimesOut() {
        SpringApplication app = new SpringApplication(MapperApplication.class);
        app.setWebApplicationType(WebApplicationType.NONE);

        assertThatThrownBy(() -> app.run(
                "--spring.kafka.bootstrap-servers=localhost:1",
                "--spring.application.name=service-mapper",
                "--kafka.config-replay.startup-timeout=3s",
                "--kafka.group.config-replay=test-unreachable-" + UUID.randomUUID(),
                "--kafka.topics.config-replay=comhub.config.v1",
                "--spring.kafka.consumer.auto-offset-reset=earliest"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Config replay did not complete");
    }
}
