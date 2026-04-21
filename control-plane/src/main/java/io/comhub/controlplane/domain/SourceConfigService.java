package io.comhub.controlplane.domain;

import io.comhub.common.config.ConfigCache;
import io.comhub.common.config.MappingConfig;
import io.comhub.controlplane.kafka.ConfigTopicPublisher;
import io.comhub.controlplane.web.dto.CreateSourceConfigRequest;
import io.comhub.controlplane.web.dto.UpdateSourceConfigRequest;
import org.springframework.stereotype.Service;

import java.util.Collection;

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
        MappingConfig config = toMappingConfig(request);
        publisher.publish(config);
        return config;
    }

    public MappingConfig update(String sourceTopic, UpdateSourceConfigRequest request) {
        MappingConfig config = toMappingConfig(sourceTopic, request);
        publisher.publish(config);
        return config;
    }

    public void delete(String sourceTopic) {
        publisher.publishTombstone(sourceTopic);
    }

    private MappingConfig toMappingConfig(CreateSourceConfigRequest request) {
        return new MappingConfig(
                request.sourceTopic(),
                request.displayName(),
                request.enabled(),
                request.configSchemaVersion() == null ? 0 : request.configSchemaVersion(),
                request.rules(),
                request.emailRecipient());
    }

    private MappingConfig toMappingConfig(String sourceTopic, UpdateSourceConfigRequest request) {
        return new MappingConfig(
                sourceTopic,
                request.displayName(),
                request.enabled(),
                request.configSchemaVersion() == null ? 0 : request.configSchemaVersion(),
                request.rules(),
                request.emailRecipient());
    }
}
