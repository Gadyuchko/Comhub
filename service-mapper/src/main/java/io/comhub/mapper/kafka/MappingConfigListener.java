package io.comhub.mapper.kafka;

import io.comhub.common.config.ConfigKey;
import io.comhub.common.config.ConfigReplayCoordinator;
import io.comhub.common.config.MappingConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Hosts the {@code @KafkaListener} for {@code comhub.config.v1} and bridges
 * record updates into the shared {@link ConfigReplayCoordinator}.
 * Contains no business logic.
 *
 * @author Roman Hadiuchko
 */
@Component
public final class MappingConfigListener {

    private final ConfigReplayCoordinator configReplayCoordinator;
    private final SourceListenerManager sourceListenerManager;

    public MappingConfigListener(ConfigReplayCoordinator configReplayCoordinator,
                                 SourceListenerManager sourceListenerManager) {
        this.configReplayCoordinator = configReplayCoordinator;
        this.sourceListenerManager = sourceListenerManager;
    }

    @KafkaListener(
            id = "config-replay",
            topics = "${kafka.topics.config-replay}",
            groupId = "${kafka.group.config-replay}",
            containerFactory = "configKafkaListenerContainerFactory")
    public void onRecord(ConsumerRecord<String, MappingConfig> record, Consumer<?, ?> consumer){
        // this is a gate for start up config replay, after startup finished we can reconcile containers normally,
        // check before applying to avoid starting duplicate container for last replayed record,
        // since this is going to be handled by orchestrator in reconcileAll()
        boolean liveRecord = configReplayCoordinator.isReplayComplete();
        configReplayCoordinator.apply(record, consumer);
        if (liveRecord) {

            ConfigKey key = ConfigKey.parse(record.key());
            if (key != null) {
                sourceListenerManager.reconcile(key.topic());
            }
        }
    }

}
