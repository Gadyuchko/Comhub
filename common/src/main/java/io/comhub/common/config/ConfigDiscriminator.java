package io.comhub.common.config;

/**
 * Declares how a source event type is identified inside a shared source topic, it can come from either header or the event payload.
 *
 * @author Roman Hadiuchko
 */
public record ConfigDiscriminator(
        String source,
        String key) {
}
