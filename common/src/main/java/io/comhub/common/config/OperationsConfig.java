package io.comhub.common.config;

import java.util.List;

/**
 * Groups operations-plane configuration that consumes promoted source attributes.
 *
 * @author Roman Hadiuchko
 */
public record OperationsConfig(
        List<PromotedAttribute> promotedAttributes,
        List<ClassificationRule> classification,
        List<RoutingRule> routing) {

    public OperationsConfig {
        promotedAttributes = promotedAttributes == null ? List.of() : List.copyOf(promotedAttributes);
        classification = classification == null ? List.of() : List.copyOf(classification);
        routing = routing == null ? List.of() : List.copyOf(routing);
    }
}
