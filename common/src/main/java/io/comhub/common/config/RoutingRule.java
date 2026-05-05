package io.comhub.common.config;

import java.util.List;

/**
 * Declares how matched source events should be routed and acted on.
 *
 * @author Roman Hadiuchko
 */
public record RoutingRule(
        String handler,
        List<Condition> conditions,
        List<RoutingAction> actions) {

    public RoutingRule {
        conditions = conditions == null ? List.of() : List.copyOf(conditions);
        actions = actions == null ? List.of() : List.copyOf(actions);
    }
}
