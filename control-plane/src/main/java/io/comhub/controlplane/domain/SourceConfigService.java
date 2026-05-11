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
import java.util.Objects;

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
        String effectiveSourceEventType = effectiveSourceEventType(request.topic(), request.sourceEventType(), request.discriminator());
        collectTopicDiscriminatorConflictErrors(request.topic(), effectiveSourceEventType, request.discriminator(), fieldErrors);
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
        collectTopicDiscriminatorConflictErrors(request.topic(), effectiveSourceEventType, request.discriminator(), fieldErrors);
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
            return;
        }

        if (discriminator.source() == DiscriminatorSource.TOPIC) {
            if (discriminator.key() != null && !discriminator.key().isBlank()) {
                fieldErrors.put("discriminator.key", "must be blank when source is 'topic'");
            }
        } else if (discriminator.key() == null || discriminator.key().isBlank()) {
            fieldErrors.put("discriminator.key", "must not be blank");
        }
    }

    // All configs on the same topic must use the same discriminator. The mapper picks any
    // one of them to read the discriminator from and then looks up ConfigKey(topic, type) in
    // O(1). If they disagreed, the lookup would silently miss records. We block the save
    // here so the user gets a clear 400 instead of a quiet skip later.
    private void collectTopicDiscriminatorConflictErrors(String topic,
                                                         String sourceEventType,
                                                         ConfigDiscriminator discriminator,
                                                         Map<String, String> fieldErrors) {
        if (topic == null || topic.isBlank() || discriminator == null || discriminator.source() == null) {
            return;
        }
        if (fieldErrors.containsKey("discriminator.source") || fieldErrors.containsKey("discriminator.key")) {
            return;
        }

        for (MappingConfig existing : cache.configsForTopic(topic)) {
            if (existing.sourceEventType().equals(sourceEventType)) {
                continue;
            }
            if (!Objects.equals(existing.discriminator(), discriminator)) {
                fieldErrors.put("discriminator",
                        "conflicts with config '" + existing.sourceEventType() + "' already on topic '" + topic
                                + "'; existing config uses " + describe(existing.discriminator())
                                + ", this uses " + describe(discriminator)
                                + ". Update or remove the other config first.");
                return;
            }
        }
    }

    private String describe(ConfigDiscriminator discriminator) {
        if (discriminator == null || discriminator.source() == null) {
            return "(none)";
        }
        if (discriminator.source() == DiscriminatorSource.TOPIC) {
            return "topic";
        }
        return discriminator.source().wireValue() + ":" + discriminator.key();
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
