package io.comhub.common.config;

/**
 * Minimal structured condition used by classification and routing configuration.
 *
 * @author Roman Hadiuchko
 */
public record Condition(
        String attribute,
        String operator,
        String value) {
}
