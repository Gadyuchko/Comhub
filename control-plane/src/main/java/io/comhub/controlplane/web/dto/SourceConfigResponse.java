package io.comhub.controlplane.web.dto;

import io.comhub.common.config.CanonicalMapping;
import io.comhub.common.config.ConfigDiscriminator;
import io.comhub.common.config.MappingConfig;
import io.comhub.common.config.OperationsConfig;

/**
 * HTTP response representation of a {@link MappingConfig} returned from the control-plane's
 * local {@code ConfigCache}. The shape mirrors the canonical record so clients of the upcoming
 * {@code /config} UI can bind fields directly without an additional adapter layer.
 *
 * @author Roman Hadiuchko
 */
public record SourceConfigResponse(
        String topic,
        String sourceEventType,
        boolean enabled,
        int configSchemaVersion,
        ConfigDiscriminator discriminator,
        CanonicalMapping mapping,
        OperationsConfig operations) {

    public static SourceConfigResponse from(MappingConfig config) {
        return new SourceConfigResponse(
                config.topic(),
                config.sourceEventType(),
                config.enabled(),
                config.configSchemaVersion(),
                config.discriminator(),
                config.mapping(),
                config.operations());
    }
}
