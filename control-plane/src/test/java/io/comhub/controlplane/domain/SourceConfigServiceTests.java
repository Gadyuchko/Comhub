package io.comhub.controlplane.domain;

import io.comhub.common.config.CanonicalMapping;
import io.comhub.common.config.ConfigCache;
import io.comhub.common.config.ConfigDiscriminator;
import io.comhub.common.config.ConfigKey;
import io.comhub.common.config.DiscriminatorSource;
import io.comhub.common.config.MappingConfig;
import io.comhub.common.config.OperationsConfig;
import io.comhub.controlplane.kafka.ConfigTopicPublisher;
import io.comhub.controlplane.web.dto.CreateSourceConfigRequest;
import io.comhub.controlplane.web.dto.UpdateSourceConfigRequest;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class SourceConfigServiceTests {

    @Test
    void createDerivesSourceEventTypeFromTopicWhenDiscriminatorSourceIsTopic() {
        ConfigTopicPublisher publisher = mock(ConfigTopicPublisher.class);
        SourceConfigService service = new SourceConfigService(new ConfigCache(), publisher);

        service.create(new CreateSourceConfigRequest(
                "orders.failed.v1",
                null,
                true,
                2,
                new ConfigDiscriminator(DiscriminatorSource.TOPIC, null),
                new CanonicalMapping(null, null, null, null, null, List.of()),
                new OperationsConfig(List.of(), List.of(), List.of())));

        ArgumentCaptor<MappingConfig> captor = ArgumentCaptor.forClass(MappingConfig.class);
        verify(publisher).publish(captor.capture());
        assertThat(captor.getValue().sourceEventType()).isEqualTo("orders.failed.v1");
        assertThat(captor.getValue().discriminator().key()).isNull();
    }

    @Test
    void createRejectsTopicDiscriminatorWithNonBlankKey() {
        ConfigTopicPublisher publisher = mock(ConfigTopicPublisher.class);
        SourceConfigService service = new SourceConfigService(new ConfigCache(), publisher);

        assertThatThrownBy(() -> service.create(new CreateSourceConfigRequest(
                "orders.failed.v1",
                null,
                true,
                2,
                new ConfigDiscriminator(DiscriminatorSource.TOPIC, "/type"),
                new CanonicalMapping(null, null, null, null, null, List.of()),
                new OperationsConfig(List.of(), List.of(), List.of()))))
                .isInstanceOf(InvalidSourceConfigException.class)
                .extracting("fieldErrors", org.assertj.core.api.InstanceOfAssertFactories.MAP)
                .containsEntry("discriminator.key", "must be blank when source is 'topic'");
        verify(publisher, never()).publish(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void createRejectsDiscriminatorThatConflictsWithExistingConfigOnSameTopic() {
        ConfigCache cache = new ConfigCache();
        cache.put(new ConfigKey("orders.v1", "order.created"),
                sample("orders.v1", "order.created",
                        new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type")));
        ConfigTopicPublisher publisher = mock(ConfigTopicPublisher.class);
        SourceConfigService service = new SourceConfigService(cache, publisher);

        assertThatThrownBy(() -> service.create(new CreateSourceConfigRequest(
                "orders.v1",
                "order.shipped",
                true,
                2,
                new ConfigDiscriminator(DiscriminatorSource.HEADER, "eventType"),
                new CanonicalMapping(null, null, null, null, null, List.of()),
                new OperationsConfig(List.of(), List.of(), List.of()))))
                .isInstanceOf(InvalidSourceConfigException.class)
                .extracting("fieldErrors", org.assertj.core.api.InstanceOfAssertFactories.MAP)
                .hasEntrySatisfying("discriminator", value ->
                        assertThat((String) value).contains("conflicts with config 'order.created'"));
        verify(publisher, never()).publish(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void createAllowsDiscriminatorThatMatchesExistingConfigOnSameTopic() {
        ConfigCache cache = new ConfigCache();
        cache.put(new ConfigKey("orders.v1", "order.created"),
                sample("orders.v1", "order.created",
                        new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type")));
        ConfigTopicPublisher publisher = mock(ConfigTopicPublisher.class);
        SourceConfigService service = new SourceConfigService(cache, publisher);

        service.create(new CreateSourceConfigRequest(
                "orders.v1",
                "order.shipped",
                true,
                2,
                new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type"),
                new CanonicalMapping(null, null, null, null, null, List.of()),
                new OperationsConfig(List.of(), List.of(), List.of())));

        verify(publisher).publish(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void updateAllowsChangingDiscriminatorOnSingleConfigTopic() {
        ConfigCache cache = new ConfigCache();
        cache.put(new ConfigKey("orders.v1", "order.created"),
                sample("orders.v1", "order.created",
                        new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type")));
        ConfigTopicPublisher publisher = mock(ConfigTopicPublisher.class);
        SourceConfigService service = new SourceConfigService(cache, publisher);

        service.update("orders.v1", "order.created", new UpdateSourceConfigRequest(
                "orders.v1",
                "order.created",
                true,
                2,
                new ConfigDiscriminator(DiscriminatorSource.HEADER, "eventType"),
                new CanonicalMapping(null, null, null, null, null, List.of()),
                new OperationsConfig(List.of(), List.of(), List.of())));

        verify(publisher).publish(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void updateRejectsDiscriminatorChangeWhenAnotherConfigOnTopicStillExists() {
        ConfigCache cache = new ConfigCache();
        cache.put(new ConfigKey("orders.v1", "order.created"),
                sample("orders.v1", "order.created",
                        new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type")));
        cache.put(new ConfigKey("orders.v1", "order.shipped"),
                sample("orders.v1", "order.shipped",
                        new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type")));
        ConfigTopicPublisher publisher = mock(ConfigTopicPublisher.class);
        SourceConfigService service = new SourceConfigService(cache, publisher);

        assertThatThrownBy(() -> service.update("orders.v1", "order.created", new UpdateSourceConfigRequest(
                "orders.v1",
                "order.created",
                true,
                2,
                new ConfigDiscriminator(DiscriminatorSource.HEADER, "eventType"),
                new CanonicalMapping(null, null, null, null, null, List.of()),
                new OperationsConfig(List.of(), List.of(), List.of()))))
                .isInstanceOf(InvalidSourceConfigException.class)
                .extracting("fieldErrors", org.assertj.core.api.InstanceOfAssertFactories.MAP)
                .hasEntrySatisfying("discriminator", value ->
                        assertThat((String) value).contains("conflicts with config 'order.shipped'"));
        verify(publisher, never()).publish(org.mockito.ArgumentMatchers.any());
    }

    private MappingConfig sample(String topic, String sourceEventType, ConfigDiscriminator discriminator) {
        return new MappingConfig(
                topic,
                sourceEventType,
                true,
                2,
                discriminator,
                new CanonicalMapping(null, null, null, null, null, List.of()),
                new OperationsConfig(List.of(), List.of(), List.of()));
    }
}
