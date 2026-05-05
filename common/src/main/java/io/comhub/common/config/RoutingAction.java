package io.comhub.common.config;

/**
 * Declares one inline routing action emitted by a routing rule.
 *
 * @author Roman Hadiuchko
 */
public record RoutingAction(
        String type,
        String channel,
        String target) {
}
