package io.comhub.common.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

import java.util.Collection;

/**
 * Bridges Spring Kafka's rebalance callbacks into {@link ConfigReplayCoordinator}.
 *
 * <p>Every service that consumes {@code comhub.config.v1} needs the same behaviour on partition
 * assignment: seek to the beginning and snapshot the current end offset so replay can be tracked
 * to completion. Those actions need the live {@link Consumer}, which the plain Kafka rebalance
 * listener interface does not hand back. Spring Kafka's {@link ConsumerAwareRebalanceListener}
 * does, so we implement that one here and forward the call through to the coordinator.
 *
 * <p>Lives in {@code common} so every consuming service can share the same small adapter instead
 * of declaring an anonymous inner class inside its own Kafka configuration.
 *
 * @author Roman Hadiuchko
 */
public final class ConfigReplayRebalanceBridge implements ConsumerAwareRebalanceListener {

    private final ConfigReplayCoordinator configReplayCoordinator;

    public ConfigReplayRebalanceBridge(ConfigReplayCoordinator configReplayCoordinator) {
        this.configReplayCoordinator = configReplayCoordinator;
    }

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        configReplayCoordinator.onPartitionsAssigned(partitions, consumer);
    }
}
