package io.comhub.common.config;

import java.util.List;

/**
 * Declares how a source event populates canonical event fields.
 *
 * @author Roman Hadiuchko
 */
public record CanonicalMapping(
        CanonicalFieldMapping occurredAt,
        CanonicalFieldMapping severity,
        CanonicalFieldMapping category,
        CanonicalFieldMapping subject,
        CanonicalFieldMapping message,
        List<AttributeMapping> attributes) {

    public CanonicalMapping {
        attributes = attributes == null ? List.of() : List.copyOf(attributes);
    }
}
