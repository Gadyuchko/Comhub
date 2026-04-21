package io.comhub.controlplane.metrics;

import io.comhub.common.config.ConfigCache;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers Micrometer gauges that surface the control-plane's local operational state to
 * Prometheus. The {@code comhub_config_cache_entries} gauge reflects the size of the in-memory
 * {@link ConfigCache} for this pod and is the primary way to detect cache drift across replicas
 * when the config topic is broadcast per-pod.
 *
 * @author Roman Hadiuchko
 */
@Configuration
public class ControlPlaneMetricsConfiguration {

    @Bean
    public MeterBinder configCacheEntriesGauge(
            ConfigCache cache,
            @Value("${spring.application.name}") String serviceName) {

        return registry -> Gauge
                .builder("comhub_config_cache_entries", cache, ConfigCache::size)
                .tag("service", serviceName)
                .description("Number of mapping entries currently held in the local config cache.")
                .register(registry);
    }
}
