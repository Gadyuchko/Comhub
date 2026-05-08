package io.comhub.mapper;

import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.SpringApplication;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * If mapper cannot read config topic on startup, it should stop.
 *
 * <p>Otherwise we would start with an empty cache and source events would be handled
 * with no real mapping rules. This test points Kafka to a local port where nothing
 * listens and uses a short timeout, so we can prove startup fails quickly.
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
