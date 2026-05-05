package io.comhub.common.config;

import java.util.List;

/**
 * Declares one classification outcome for matched source-event conditions.
 *
 * @author Roman Hadiuchko
 */
public record ClassificationRule(
        String code,
        String handler,
        List<Condition> conditions) {

    public ClassificationRule {
        conditions = conditions == null ? List.of() : List.copyOf(conditions);
    }
}
