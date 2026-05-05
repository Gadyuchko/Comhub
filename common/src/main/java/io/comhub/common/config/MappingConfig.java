package io.comhub.common.config;

/**
 * Per-source-event mapping configuration replicated from {@code comhub.config.v1} into each
 * service's in-memory cache.
 *
 * @author Roman Hadiuchko
 */
public record MappingConfig(
        String topic,
        String sourceEventType,
        boolean enabled,
        int configSchemaVersion,
        ConfigDiscriminator discriminator,
        CanonicalMapping mapping,
        OperationsConfig operations) {

    public MappingConfig {
        if (configSchemaVersion == 0) {
            configSchemaVersion = 2;
        }
    }
}
