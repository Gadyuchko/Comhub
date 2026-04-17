package io.comhub.common.config;

/**
 * Mapping rule applied by the mapper service when transforming a source event into the canonical
 * event model. Kept intentionally minimal so the configuration contract can be committed to now
 * without leaking the rule shape across module boundaries.
 *
 * @author Roman Hadiuchko
 */
public record MappingRule() {
}
