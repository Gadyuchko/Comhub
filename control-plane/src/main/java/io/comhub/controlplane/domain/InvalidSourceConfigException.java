package io.comhub.controlplane.domain;

import java.util.Map;

/**
 * Raised when the source-config request shape is incomplete or inconsistent.
 *
 * @author Roman Hadiuchko
 */
public class InvalidSourceConfigException extends RuntimeException {

    private final Map<String, String> fieldErrors;

    public InvalidSourceConfigException(Map<String, String> fieldErrors) {
        super("Request validation failed");
        this.fieldErrors = Map.copyOf(fieldErrors);
    }

    public Map<String, String> getFieldErrors() {
        return fieldErrors;
    }
}
