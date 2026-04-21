package io.comhub.controlplane.web.dto;

import io.comhub.common.config.MappingRule;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.List;

/**
 * Request body for {@code POST /api/source-configs}. Carries the full source configuration
 * payload including {@code sourceTopic}, which is treated as the MVP resource identifier and
 * becomes the Kafka message key on publish to {@code comhub.config.v1}.
 *
 * @author Roman Hadiuchko
 */
public record CreateSourceConfigRequest(
        @NotBlank String sourceTopic,
        @NotBlank String displayName,
        @NotNull Boolean enabled,
        Integer configSchemaVersion,
        @NotNull @Valid List<MappingRule> rules,
        @NotBlank @Email String emailRecipient) {
}
