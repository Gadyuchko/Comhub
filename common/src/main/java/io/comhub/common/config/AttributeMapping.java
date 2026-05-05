package io.comhub.common.config;

/**
 * Maps one promoted attribute from source data into the canonical event attributes bag.
 *
 * @author Roman Hadiuchko
 */
public record AttributeMapping(
        String targetAttribute,
        String source) {
}
