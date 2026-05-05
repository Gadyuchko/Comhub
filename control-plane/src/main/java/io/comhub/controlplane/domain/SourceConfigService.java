package io.comhub.controlplane.domain;

import io.comhub.common.config.ConfigCache;
import io.comhub.common.config.ConfigDiscriminator;
import io.comhub.common.config.ConfigKey;
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
 * <p>Reads return the current snapshot of the local {@link ConfigCache}, which is itself a
 * materialized view of {@code comhub.config.v1}. Mutations delegate to {@link
 * ConfigTopicPublisher} and intentionally never touch the cache directly: cache transitions
 * happen only when the control-plane's own config listener observes the record on Kafka.
 *
 * <p>Any publish failure or timeout surfaces to the caller as {@link ConfigPublishException},
 * which the web layer maps to {@code 503}. This preserves the architectural invariant that
 * HTTP success implies a durable record on the config topic.
 *
 * <p>Discriminator-conflict checks read from the local cache, which is eventually consistent.
 * Two concurrent writes that introduce conflicting discriminators on the same topic can both
 * pass this check and both publish; the listener will then skip whichever record loses the
 * race when materializing it. Cache state stays correct, but the losing caller observed an
 * optimistic {@code 201}/{@code 200}. A stronger publish-ack-and-observe round-trip is
 * deliberately out of MVP scope.
 *
 * @author Roman Hadiuchko
 */
@Service
public class SourceConfigService {

    private final ConfigCache cache;
    private final ConfigTopicPublisher publisher;

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
        if (!fieldErrors.isEmpty()) {
            throw new InvalidSourceConfigException(fieldErrors);
        }

        MappingConfig config = toMappingConfig(request);
        ensureConsistentDiscriminator(config, null);
        publisher.publish(config);
        return config;
    }

    public MappingConfig update(String topic, String sourceEventType, UpdateSourceConfigRequest request) {
        Map<String, String> fieldErrors = new LinkedHashMap<>();
        if (!topic.equals(request.topic())) {
            fieldErrors.put("topic", "must match path topic");
        }
        if (!sourceEventType.equals(request.sourceEventType())) {
            fieldErrors.put("sourceEventType", "must match path sourceEventType");
        }
        collectDiscriminatorErrors(request.discriminator(), fieldErrors);
        if (!fieldErrors.isEmpty()) {
            throw new InvalidSourceConfigException(fieldErrors);
        }

        MappingConfig config = toMappingConfig(request);
        ensureConsistentDiscriminator(config, new ConfigKey(topic, sourceEventType));
        publisher.publish(config);
        return config;
    }

    public void delete(String topic, String sourceEventType) {
        publisher.publishTombstone(new ConfigKey(topic, sourceEventType));
    }

    private MappingConfig toMappingConfig(CreateSourceConfigRequest request) {
        return new MappingConfig(
                request.topic(),
                request.sourceEventType(),
                request.enabled(),
                request.configSchemaVersion() == null ? 0 : request.configSchemaVersion(),
                request.discriminator(),
                request.mapping(),
                request.operations());
    }

    private MappingConfig toMappingConfig(UpdateSourceConfigRequest request) {
        return new MappingConfig(
                request.topic(),
                request.sourceEventType(),
                request.enabled(),
                request.configSchemaVersion() == null ? 0 : request.configSchemaVersion(),
                request.discriminator(),
                request.mapping(),
                request.operations());
    }

    // Top-level nullability and blankness for topic, sourceEventType, discriminator,
    // mapping, and operations is enforced by Jakarta validation on the request DTOs.
    // This method validates nested discriminator content, which is not annotated on the
    // shared common record so the validation dependency does not leak into the common module.
    private void collectDiscriminatorErrors(ConfigDiscriminator discriminator, Map<String, String> fieldErrors) {
        if (discriminator == null) {
            return;
        }

        String source = discriminator.source();
        if (source == null || source.isBlank()) {
            fieldErrors.put("discriminator.source", "must not be blank");
        } else if (!"header".equals(source) && !"payload".equals(source)) {
            fieldErrors.put("discriminator.source", "must be either 'header' or 'payload'");
        }

        if (discriminator.key() == null || discriminator.key().isBlank()) {
            fieldErrors.put("discriminator.key", "must not be blank");
        }
    }

    private void ensureConsistentDiscriminator(MappingConfig config, ConfigKey currentKey) {
        cache.configsForTopic(config.topic()).stream()
                .filter(existing -> currentKey == null
                        || !new ConfigKey(existing.topic(), existing.sourceEventType()).equals(currentKey))
                .map(MappingConfig::discriminator)
                .filter(existing -> existing != null)
                .filter(existing -> !existing.equals(config.discriminator()))
                .findFirst()
                .ifPresent(existing -> {
                    throw new DiscriminatorConflictException(
                            "Topic '%s' already uses discriminator %s/%s"
                                    .formatted(config.topic(), existing.source(), existing.key()));
                });
    }
}
