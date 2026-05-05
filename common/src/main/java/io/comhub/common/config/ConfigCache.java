package io.comhub.common.config;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory cache of {@link MappingConfig} entries keyed by source topic and source-event type.
 *
 * <p>Writes arrive on a single thread — the Kafka listener that consumes {@code comhub.config.v1}.
 * Reads may happen from any thread; the underlying {@code ConcurrentHashMap} makes reads and
 * writes safe to interleave without additional locking.
 *
 * @author Roman Hadiuchko
 */
public final class ConfigCache {

    private final ConcurrentHashMap<ConfigKey, MappingConfig> map = new ConcurrentHashMap<>();

    public MappingConfig get(ConfigKey key) {
        return map.get(key);
    }

    public void put(ConfigKey key, MappingConfig config) {
        map.put(key, config);
    }

    public void remove(ConfigKey key) {
        map.remove(key);
    }

    public int size() {
        return map.size();
    }

    /**
     * Returns an immutable snapshot of the current cache entries. Intended for list-style reads
     * such as the control-plane's {@code GET /api/source-configs} endpoint. Hot-path consumers
     * should continue to use {@link #get(ConfigKey)} so they avoid allocating a snapshot per event.
     */
    public Collection<MappingConfig> values() {
        return List.copyOf(map.values());
    }

    public Collection<MappingConfig> configsForTopic(String topic) {
        return map.entrySet().stream()
                .filter(entry -> entry.getKey().topic().equals(topic))
                .map(java.util.Map.Entry::getValue)
                .toList();
    }
}
