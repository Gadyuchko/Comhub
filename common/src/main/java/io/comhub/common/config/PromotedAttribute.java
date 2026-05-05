package io.comhub.common.config;

/**
 * Describes an attribute promoted for downstream routing, delivery, or dashboard use.
 *
 * @author Roman Hadiuchko
 */
public record PromotedAttribute(
        String sourceAttribute,
        String targetAttribute) {
}
