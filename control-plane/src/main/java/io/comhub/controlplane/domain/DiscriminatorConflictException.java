package io.comhub.controlplane.domain;

/**
 * Raised when two configs on the same topic declare different discriminators.
 *
 * @author Roman Hadiuchko
 */
public class DiscriminatorConflictException extends RuntimeException {

    public DiscriminatorConflictException(String message) {
        super(message);
    }
}
