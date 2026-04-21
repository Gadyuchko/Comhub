package io.comhub.controlplane.kafka;

import io.comhub.common.config.ConfigReplayCoordinator;
import io.comhub.common.config.MappingConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Hosts the {@code @KafkaListener} for {@code comhub.config.v1} and bridges
 * Spring Kafka's per-bean {@link ConsumerSeekAware} callbacks into the shared
 * {@link ConfigReplayCoordinator}. Contains no business logic.
 *
 * @author Roman Hadiuchko
 */
@Component
public final class ControlConfigListener {

    private final ConfigReplayCoordinator configReplayCoordinator;

    public ControlConfigListener(ConfigReplayCoordinator configReplayCoordinator) {
        this.configReplayCoordinator = configReplayCoordinator;
    }

    @KafkaListener(
            id = "config-replay",
            topics = "${kafka.topics.config-replay}",
            groupId = "${kafka.group.config-replay}",
            containerFactory = "configKafkaListenerContainerFactory")
    public void onRecord(ConsumerRecord<String, MappingConfig> record, Consumer<?, ?> consumer){
        configReplayCoordinator.apply(record, consumer);
    }

}
