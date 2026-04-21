package io.comhub.controlplane.domain;

/**
 * Signals that a configuration mutation could not be durably published to {@code comhub.config.v1}.
 *
 * <p>Thrown by the domain layer when the underlying Kafka publish either failed or timed out.
 * The web layer translates this exception into an RFC 7807 {@code 503 Service Unavailable}
 * response while the local {@code ConfigCache} state is preserved unchanged, per the
 * architecture rule that config topic observation — not controller input — is the only
 * authoritative source of cache mutations.
 *
 * @author Roman Hadiuchko
 */
public class ConfigPublishException extends RuntimeException {

    public ConfigPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
