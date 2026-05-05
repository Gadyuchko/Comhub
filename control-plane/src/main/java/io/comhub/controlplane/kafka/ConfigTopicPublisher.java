package io.comhub.controlplane.kafka;

import io.comhub.common.config.ConfigKey;
import io.comhub.common.config.MappingConfig;

/**
 * Publishes source-configuration mutations to {@code comhub.config.v1}.
 *
 * <p>{@link #publish(MappingConfig)} writes an upsert record keyed by the config's composite
 * identity. {@link #publishTombstone(ConfigKey)} writes a {@code null}-valued record on the same key so that
 * downstream compacted-topic consumers remove the entry during their next replay window.
 *
 * <p>Implementations must not update the control-plane's local {@code ConfigCache}. Cache state
 * changes are driven exclusively by the config-topic listener observing the record on Kafka.
 * Implementations must surface publish failures and timeouts as {@link
 * io.comhub.controlplane.domain.ConfigPublishException} so the web layer can emit the defined
 * {@code 503} response while leaving local cache untouched.
 *
 * @author Roman Hadiuchko
 */
public interface ConfigTopicPublisher {

    void publish(MappingConfig config);

    void publishTombstone(ConfigKey key);
}
