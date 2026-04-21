package io.comhub.controlplane.web.dto;

import io.comhub.common.config.MappingConfig;
import io.comhub.common.config.MappingRule;

import java.util.List;

/**
 * HTTP response representation of a {@link MappingConfig} returned from the control-plane's
 * local {@code ConfigCache}. The shape mirrors the canonical record so clients of the upcoming
 * {@code /config} UI can bind fields directly without an additional adapter layer.
 *
 * @author Roman Hadiuchko
 */
public record SourceConfigResponse(
        String sourceTopic,
        String displayName,
        boolean enabled,
        int configSchemaVersion,
        List<MappingRule> rules,
        String emailRecipient) {

    public static SourceConfigResponse from(MappingConfig config) {
        return new SourceConfigResponse(
                config.sourceTopic(),
                config.displayName(),
                config.enabled(),
                config.configSchemaVersion(),
                config.rules(),
                config.emailRecipient());
    }
}
