package io.comhub.controlplane.kafka;

import io.comhub.common.config.MappingConfig;
import io.comhub.controlplane.domain.ConfigPublishException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Kafka-backed implementation of {@link ConfigTopicPublisher}.
 *
 * <p>Writes source-configuration mutations to {@code comhub.config.v1} using the shared
 * {@link KafkaTemplate}. Each record is keyed by the source topic so log compaction keeps the
 * latest state per configuration and consumers can materialize the record into their local
 * {@code ConfigCache} under the same key. Tombstones reuse the same key with a
 * {@code null} value, which Kafka treats as a delete marker on the compacted topic.
 *
 * <p>The HTTP request thread blocks on the send future up to the configured timeout so that
 * success or failure is known before the controller responds. Any failure — broker error,
 * timeout, or interruption — is rethrown as {@link ConfigPublishException}, which the web
 * layer maps to {@code 503 Service Unavailable}. The control-plane's local {@code ConfigCache}
 * is never touched here; cache transitions happen only when the config-topic listener observes
 * the record on Kafka.
 *
 * @author Roman Hadiuchko
 */
@Component
public class ConfigTopicPublisherImpl implements ConfigTopicPublisher {

    private static final Logger log = LoggerFactory.getLogger(ConfigTopicPublisherImpl.class);

    private final KafkaTemplate<String, MappingConfig> kafkaTemplate;
    private final String configTopic;
    private final Duration timeout;

    public ConfigTopicPublisherImpl(KafkaTemplate<String, MappingConfig> kafkaTemplate,
                                    @Value("${kafka.topics.config}") String configTopic,
                                    @Value("${kafka.publisher.timeout:5s}") Duration timeout) {
        this.kafkaTemplate = kafkaTemplate;
        this.configTopic = configTopic;
        this.timeout = timeout;
    }

    @Override
    public void publish(MappingConfig config) {
        String sourceTopic = config.sourceTopic();
        try {
            kafkaTemplate.send(configTopic, sourceTopic, config)
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            log.debug("Published config update for source {}", sourceTopic);
        } catch (TimeoutException ex) {
            throw new ConfigPublishException("Config publish timed out after " + timeout + " for source " + sourceTopic, ex);
        } catch (ExecutionException ex) {
            throw new ConfigPublishException("Config publish failed for source " + sourceTopic, ex.getCause());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ConfigPublishException("Config publish was interrupted for source " + sourceTopic, ex);
        }
    }

    @Override
    public void publishTombstone(String sourceTopic) {
        try {
            kafkaTemplate.send(configTopic, sourceTopic, null)
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            log.debug("Published config tombstone for source {}", sourceTopic);
        } catch (TimeoutException ex) {
            throw new ConfigPublishException(
                    "Config tombstone publish timed out after " + timeout + " for source " + sourceTopic, ex);
        } catch (ExecutionException ex) {
            throw new ConfigPublishException(
                    "Config tombstone publish failed for source " + sourceTopic, ex.getCause());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ConfigPublishException(
                    "Config tombstone publish was interrupted for source " + sourceTopic, ex);
        }
    }
}
