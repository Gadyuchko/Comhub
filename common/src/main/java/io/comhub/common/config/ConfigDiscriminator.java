package io.comhub.common.config;

/**
 * Declares where mapper should look to understand which config can process a source event.
 *
 * @author Roman Hadiuchko
 */
public record ConfigDiscriminator(
        DiscriminatorSource source,
        String key) {
}
