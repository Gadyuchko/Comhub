package io.comhub.common.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Locale;

/**
 * Defines where mapper should look for source event type value.
 *
 * @author Roman Hadiuchko
 */
public enum DiscriminatorSource {
    HEADER("header"),
    PAYLOAD("payload"),
    TOPIC("topic");

    private final String wireValue;

    DiscriminatorSource(String wireValue) {
        this.wireValue = wireValue;
    }

    @JsonCreator
    public static DiscriminatorSource fromWireValue(String value) {
        if (value == null) {
            return null;
        }

        String normalized = value.trim().toLowerCase(Locale.ROOT);
        return Arrays.stream(values())
                .filter(source -> source.wireValue.equals(normalized))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown discriminator source: " + value));
    }

    @JsonValue
    public String wireValue() {
        return wireValue;
    }
}
