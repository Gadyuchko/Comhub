package io.comhub.controlplane.domain;

import io.comhub.common.config.ConfigCache;
import io.comhub.common.config.ConfigDiscriminator;
import io.comhub.common.config.ConfigKey;
import io.comhub.common.config.DiscriminatorSource;
import io.comhub.common.config.MappingConfig;
import io.comhub.controlplane.kafka.ConfigTopicPublisher;
import io.comhub.controlplane.web.dto.CreateSourceConfigRequest;
import io.comhub.controlplane.web.dto.UpdateSourceConfigRequest;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Orchestrates source-configuration reads and mutations for the control-plane HTTP API.
 *
 * <p>Reads return the current snapshot of the local config cache, which is itself a
 * materialized view of {@code comhub.config.v1}. Mutations delegate to {@link
 * ConfigTopicPublisher} and intentionally never touch the cache directly: cache transitions
 * happen only when the control-plane's own config listener observes the record on Kafka.
 *
 * <p>Any publish failure or timeout surfaces to the caller as {@link ConfigPublishException},
 * which the web layer maps to {@code 503}. This preserves the architectural invariant that
 * HTTP success implies a durable record on the config topic.
 *
 * @author Roman Hadiuchko
 */
@Service
public class SourceConfigService {

    private final ConfigTopicPublisher publisher;
    private final ConfigCache cache;

    public SourceConfigService(ConfigCache cache, ConfigTopicPublisher publisher) {
        this.cache = cache;
        this.publisher = publisher;
    }

    public Collection<MappingConfig> listAll() {
        return cache.values();
    }

    public MappingConfig create(CreateSourceConfigRequest request) {
        Map<String, String> fieldErrors = new LinkedHashMap<>();
        collectDiscriminatorErrors(request.discriminator(), fieldErrors);
        collectSourceEventTypeErrors(request.sourceEventType(), request.discriminator(), fieldErrors);
        if (!fieldErrors.isEmpty()) {
            throw new InvalidSourceConfigException(fieldErrors);
        }

        MappingConfig config = toMappingConfig(request);
        publisher.publish(config);
        return config;
    }

    public MappingConfig update(String topic, String sourceEventType, UpdateSourceConfigRequest request) {
        Map<String, String> fieldErrors = new LinkedHashMap<>();
        if (!topic.equals(request.topic())) {
            fieldErrors.put("topic", "must match path topic");
        }
        String effectiveSourceEventType = effectiveSourceEventType(request.topic(), request.sourceEventType(), request.discriminator());
        if (!sourceEventType.equals(effectiveSourceEventType)) {
            fieldErrors.put("sourceEventType", "must match path sourceEventType");
        }
        collectDiscriminatorErrors(request.discriminator(), fieldErrors);
        collectSourceEventTypeErrors(request.sourceEventType(), request.discriminator(), fieldErrors);
        if (!fieldErrors.isEmpty()) {
            throw new InvalidSourceConfigException(fieldErrors);
        }

        MappingConfig config = toMappingConfig(request);
        publisher.publish(config);
        return config;
    }

    public void delete(String topic, String sourceEventType) {
        publisher.publishTombstone(new ConfigKey(topic, sourceEventType));
    }

    private MappingConfig toMappingConfig(CreateSourceConfigRequest request) {
        return new MappingConfig(
                request.topic(),
                effectiveSourceEventType(request.topic(), request.sourceEventType(), request.discriminator()),
                request.enabled(),
                request.configSchemaVersion() == null ? 0 : request.configSchemaVersion(),
                request.discriminator(),
                request.mapping(),
                request.operations());
    }

    private MappingConfig toMappingConfig(UpdateSourceConfigRequest request) {
        return new MappingConfig(
                request.topic(),
                effectiveSourceEventType(request.topic(), request.sourceEventType(), request.discriminator()),
                request.enabled(),
                request.configSchemaVersion() == null ? 0 : request.configSchemaVersion(),
                request.discriminator(),
                request.mapping(),
                request.operations());
    }

    // Top-level nullability and blankness for topic, discriminator,
    // mapping, and operations is enforced by Jakarta validation on the request DTOs.
    // This method validates nested discriminator content, which is not annotated on the
    // shared common record so the validation dependency does not leak into the common module.
    private void collectDiscriminatorErrors(ConfigDiscriminator discriminator, Map<String, String> fieldErrors) {
        if (discriminator == null) {
            return;
        }

        if (discriminator.source() == null) {
            fieldErrors.put("discriminator.source", "must not be blank");
        }

        if (discriminator.source() != null
                && discriminator.source() != DiscriminatorSource.TOPIC
                && (discriminator.key() == null || discriminator.key().isBlank())) {
            fieldErrors.put("discriminator.key", "must not be blank");
        }
    }

    private void collectSourceEventTypeErrors(String sourceEventType,
                                              ConfigDiscriminator discriminator,
                                              Map<String, String> fieldErrors) {
        if (isTopicDiscriminator(discriminator)) {
            return;
        }

        if (sourceEventType == null || sourceEventType.isBlank()) {
            fieldErrors.put("sourceEventType", "must not be blank");
        }
    }

    private String effectiveSourceEventType(String topic, String sourceEventType, ConfigDiscriminator discriminator) {
        if (isTopicDiscriminator(discriminator)) {
            return topic;
        }
        return sourceEventType;
    }

    private boolean isTopicDiscriminator(ConfigDiscriminator discriminator) {
        return discriminator != null && discriminator.source() == DiscriminatorSource.TOPIC;
    }
}
