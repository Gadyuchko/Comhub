package io.comhub.controlplane.web.dto;

import io.comhub.common.config.CanonicalMapping;
import io.comhub.common.config.ConfigDiscriminator;
import io.comhub.common.config.OperationsConfig;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * Request body for {@code PUT /api/source-configs/{topic}/{sourceEventType}}.
 *
 * @author Roman Hadiuchko
 */
public record UpdateSourceConfigRequest(
        @NotBlank String topic,
        @NotBlank String sourceEventType,
        @NotNull Boolean enabled,
        Integer configSchemaVersion,
        @NotNull ConfigDiscriminator discriminator,
        @NotNull CanonicalMapping mapping,
        @NotNull OperationsConfig operations) {
}
