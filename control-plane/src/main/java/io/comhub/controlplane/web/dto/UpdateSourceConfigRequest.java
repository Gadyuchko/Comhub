package io.comhub.controlplane.web.dto;

import io.comhub.common.config.MappingRule;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.List;

/**
 * Request body for {@code PUT /api/source-configs/{id}}. The {@code sourceTopic} identifier is
 * taken from the path and intentionally excluded from the body so clients cannot accidentally
 * rename a configuration in-place.
 *
 * @author Roman Hadiuchko
 */
public record UpdateSourceConfigRequest(
        @NotBlank String displayName,
        @NotNull Boolean enabled,
        Integer configSchemaVersion,
        @NotNull @Valid List<MappingRule> rules,
        @NotBlank @Email String emailRecipient) {
}
