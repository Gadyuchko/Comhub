package io.comhub.mapper.domain;

/**
 * Signals that a source record cannot be transformed by the mapper.
 *
 * @author Roman Hadiuchko
 */
public class MappingFailureException extends RuntimeException {

    private final String reason;

    public MappingFailureException(String reason) {
        super(reason);
        this.reason = reason;
    }

    public MappingFailureException(String reason, Throwable cause) {
        super(reason, cause);
        this.reason = reason;
    }

    public String reason() {
        return reason;
    }
}
