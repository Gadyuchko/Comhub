package io.comhub.common.config;

import java.util.List;

/**
 * Per-source mapping configuration replicated from {@code comhub.config.v1} into each service's
 * in-memory cache. Published by the control plane and consumed by processing services to resolve
 * how a source event is transformed and delivered.
 *
 * <p>If {@code configSchemaVersion} is not set, the compact constructor falls back to {@code 1}.
 * The {@code rules} list is wrapped as immutable so the same instance is safe to share across
 * threads.
 *
 * @author Roman Hadiuchko
 */
public record MappingConfig(
        String sourceTopic,
        String displayName,
        boolean enabled,
        int configSchemaVersion,
        List<MappingRule> rules,
        String emailRecipient) {

    public MappingConfig {
        if (configSchemaVersion == 0) {
            configSchemaVersion = 1;
        }

        rules = rules == null ? List.of() : List.copyOf(rules);
    }
}
