package io.comhub.common.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

class ConfigReplayCoordinatorTests {

    private static final String TOPIC = "comhub.config.v1";

    @Test
    void appliesValidRecordToCache() {
        ConfigCache cache = new ConfigCache();
        ConfigReplayCoordinator coordinator = new ConfigReplayCoordinator(cache);

        Consumer<?, ?> consumer = consumerWithEndOffset(0L);
        deliver(coordinator, consumer, configRecord(0L, "orders.v1", "order.created",
                new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type")));

        ConfigKey key = new ConfigKey("orders.v1", "order.created");
        assertThat(cache.get(key)).isNotNull();
    }

    @Test
    void rejectsRecordWithMissingDiscriminator() {
        ConfigCache cache = new ConfigCache();
        ConfigReplayCoordinator coordinator = new ConfigReplayCoordinator(cache);

        Consumer<?, ?> consumer = consumerWithEndOffset(0L);
        deliver(coordinator, consumer, configRecord(0L, "orders.v1", "order.created", null));

        assertThat(cache.size()).isZero();
    }

    @Test
    void rejectsHeaderDiscriminatorWithBlankKey() {
        ConfigCache cache = new ConfigCache();
        ConfigReplayCoordinator coordinator = new ConfigReplayCoordinator(cache);

        Consumer<?, ?> consumer = consumerWithEndOffset(0L);
        deliver(coordinator, consumer, configRecord(0L, "orders.v1", "order.created",
                new ConfigDiscriminator(DiscriminatorSource.HEADER, "")));

        assertThat(cache.size()).isZero();
    }

    @Test
    void rejectsTopicDiscriminatorWithNonBlankKey() {
        ConfigCache cache = new ConfigCache();
        ConfigReplayCoordinator coordinator = new ConfigReplayCoordinator(cache);

        Consumer<?, ?> consumer = consumerWithEndOffset(0L);
        deliver(coordinator, consumer, configRecord(0L, "orders.failed.v1", "orders.failed.v1",
                new ConfigDiscriminator(DiscriminatorSource.TOPIC, "/type")));

        assertThat(cache.size()).isZero();
    }

    @Test
    void rejectsTopicDiscriminatorWhenSourceEventTypeDoesNotMatchTopic() {
        ConfigCache cache = new ConfigCache();
        ConfigReplayCoordinator coordinator = new ConfigReplayCoordinator(cache);

        Consumer<?, ?> consumer = consumerWithEndOffset(0L);
        deliver(coordinator, consumer, configRecord(0L, "orders.failed.v1", "different-event",
                new ConfigDiscriminator(DiscriminatorSource.TOPIC, null)));

        assertThat(cache.size()).isZero();
    }

    @Test
    void rejectsRecordWhoseDiscriminatorConflictsWithExistingConfigOnSameTopic() {
        ConfigCache cache = new ConfigCache();
        ConfigReplayCoordinator coordinator = new ConfigReplayCoordinator(cache);

        Consumer<?, ?> consumer = consumerWithEndOffset(2L);
        deliver(coordinator, consumer, configRecord(0L, "orders.v1", "order.created",
                new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type")));
        deliver(coordinator, consumer, configRecord(1L, "orders.v1", "order.shipped",
                new ConfigDiscriminator(DiscriminatorSource.HEADER, "eventType")));

        assertThat(cache.size()).isEqualTo(1);
        assertThat(cache.get(new ConfigKey("orders.v1", "order.shipped"))).isNull();
    }

    @Test
    void allowsUpdatingDiscriminatorOnSingleConfigTopic() {
        ConfigCache cache = new ConfigCache();
        ConfigReplayCoordinator coordinator = new ConfigReplayCoordinator(cache);

        Consumer<?, ?> consumer = consumerWithEndOffset(0L);
        deliver(coordinator, consumer, configRecord(0L, "orders.v1", "order.created",
                new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type")));

        deliver(coordinator, mock(Consumer.class), configRecord(1L, "orders.v1", "order.created",
                new ConfigDiscriminator(DiscriminatorSource.HEADER, "eventType")));

        ConfigKey key = new ConfigKey("orders.v1", "order.created");
        assertThat(cache.get(key).discriminator().source()).isEqualTo(DiscriminatorSource.HEADER);
    }

    @Test
    void tombstoneRemovesEntry() {
        ConfigCache cache = new ConfigCache();
        ConfigReplayCoordinator coordinator = new ConfigReplayCoordinator(cache);

        Consumer<?, ?> consumer = consumerWithEndOffset(0L);
        deliver(coordinator, consumer, configRecord(0L, "orders.v1", "order.created",
                new ConfigDiscriminator(DiscriminatorSource.PAYLOAD, "/type")));

        deliver(coordinator, mock(Consumer.class), tombstoneRecord(1L, "orders.v1", "order.created"));

        assertThat(cache.get(new ConfigKey("orders.v1", "order.created"))).isNull();
    }

    @SuppressWarnings("unchecked")
    private static Consumer<?, ?> consumerWithEndOffset(long endOffset) {
        Consumer<Object, Object> consumer = mock(Consumer.class);
        given(consumer.endOffsets(any(Set.class))).willReturn(Map.of(new TopicPartition(TOPIC, 0), endOffset));
        return consumer;
    }

    private static void deliver(ConfigReplayCoordinator coordinator,
                                Consumer<?, ?> consumer,
                                ConsumerRecord<String, MappingConfig> record) {
        coordinator.apply(record, consumer);
    }

    private static ConsumerRecord<String, MappingConfig> configRecord(long offset,
                                                                      String topic,
                                                                      String sourceEventType,
                                                                      ConfigDiscriminator discriminator) {
        MappingConfig value = new MappingConfig(
                topic,
                sourceEventType,
                true,
                2,
                discriminator,
                new CanonicalMapping(null, null, null, null, null, List.of()),
                new OperationsConfig(List.of(), List.of(), List.of()));
        return new ConsumerRecord<>(TOPIC, 0, offset, new ConfigKey(topic, sourceEventType).asRecordKey(), value);
    }

    private static ConsumerRecord<String, MappingConfig> tombstoneRecord(long offset, String topic, String sourceEventType) {
        return new ConsumerRecord<>(TOPIC, 0, offset, new ConfigKey(topic, sourceEventType).asRecordKey(), null);
    }
}
