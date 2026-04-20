package io.comhub.mapper.kafka;

import io.comhub.common.config.ConfigCache;
import io.comhub.common.config.ConfigReplayCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
/**
 * Fails service startup if config replay does not reach end-of-topic within the
 * configured timeout. Runs after all listener containers have started; by that
 * time the config listener is polling and the coordinator's latch will count
 * down on its own. If the latch doesn't release in time, we throw to abort the
 * Spring Boot startup — no snapshot fallback, visible failure per the story's
 * Kafka-unavailable acceptance criterion.
 *
 * @author Roman Hadiuchko
 */
@Component
public final class MapperListenerStartupOrchestrator implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(MapperListenerStartupOrchestrator.class);

    private final ConfigReplayCoordinator coordinator;
    private final ConfigCache cache;
    private final Duration startupTimeout;

    public MapperListenerStartupOrchestrator(ConfigReplayCoordinator coordinator,
                                             ConfigCache cache,
                                             @Value("${kafka.config-replay.startup-timeout:30s}")
                                             Duration startupTimeout) {
        this.coordinator = coordinator;
        this.cache = cache;
        this.startupTimeout = startupTimeout;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        boolean synced = coordinator.awaitEndOfReplay(startupTimeout);
        if (synced) {
            log.info("Config replay completed; cache holds {} mapping(s).", cache.size());
            return;
        }
        log.error("Config replay did not complete within {} — aborting startup.", startupTimeout);
        throw new IllegalStateException(
                "Config replay did not complete within " + startupTimeout + "; aborting startup.");
    }
}


