package io.comhub.common.config;

import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory cache of {@link MappingConfig} entries keyed by source topic name. Every service that
 * consumes {@code comhub.config.v1} materializes the topic into an instance of this cache so that
 * hot-path reads are direct {@link ConcurrentHashMap} lookups with no network or serialization.
 *
 * <p>Writes arrive on a single thread — the Kafka listener that consumes {@code comhub.config.v1}.
 * Reads may happen from any thread; the underlying {@code ConcurrentHashMap} makes reads and
 * writes safe to interleave without additional locking.
 *
 * @author Roman Hadiuchko
 */
public final class ConfigCache {

    private final ConcurrentHashMap<String, MappingConfig> map = new ConcurrentHashMap<>();

    public MappingConfig get(String sourceTopic) {
        return map.get(sourceTopic);
    }

    public void put(String sourceTopic, MappingConfig config) {
        map.put(sourceTopic, config);
    }

    public void remove(String sourceTopic) {
        map.remove(sourceTopic);
    }

    public int size() {
        return map.size();
    }
}
