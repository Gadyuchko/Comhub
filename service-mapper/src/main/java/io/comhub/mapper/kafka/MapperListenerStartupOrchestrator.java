package io.comhub.mapper.kafka;

import io.comhub.common.config.ConfigCache;
import io.comhub.common.config.ConfigReplayCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Brings the mapper to its initial steady state after Spring context startup.
 *
 * <p>Waits on {@link ConfigReplayCoordinator#awaitEndOfReplay} until the config topic has
 * been fully replayed into the cache, or the configured timeout elapses. On timeout the
 * orchestrator throws to abort Spring Boot startup — there is no snapshot fallback, the
 * failure is visible.
 *
 * <p>Once replay completes successfully, calls {@link SourceListenerManager#reconcileAll()}
 * to build and start a Kafka consumer container for every enabled source topic in the
 * cache. From that point, {@code MappingConfigListener} drives per-record reconciliation
 * for any live config changes that follow.
 *
 * @author Roman Hadiuchko
 */
@Component
public final class MapperListenerStartupOrchestrator implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(MapperListenerStartupOrchestrator.class);

    private final ConfigReplayCoordinator coordinator;
    private final ConfigCache cache;
    private final Duration startupTimeout;
    private final SourceListenerManager sourceListenerManager;

    public MapperListenerStartupOrchestrator(ConfigReplayCoordinator coordinator,
                                             ConfigCache cache,
                                             @Value("${kafka.config-replay.startup-timeout:30s}")
                                             Duration startupTimeout,
                                             SourceListenerManager sourceListenerManager) {
        this.coordinator = coordinator;
        this.cache = cache;
        this.startupTimeout = startupTimeout;
        this.sourceListenerManager = sourceListenerManager;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        boolean synced = coordinator.awaitEndOfReplay(startupTimeout);
        if (!synced) {
            log.error("Config replay did not complete within {} — aborting startup.", startupTimeout);
            throw new IllegalStateException(
                    "Config replay did not complete within " + startupTimeout + "; aborting startup.");
        }
        log.info("Config replay completed; cache holds {} mapping(s). Starting containers for sourceEvent listeners.", cache.size());
        sourceListenerManager.reconcileAll();
    }
}


