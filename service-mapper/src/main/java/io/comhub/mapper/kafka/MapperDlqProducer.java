package io.comhub.mapper.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static io.comhub.common.event.EventHeaders.*;

/**
 * Publishes source records that the mapper cannot transform to the dead letter topic.
 *
 * @author Roman Hadiuchko
 */
@Component
public class MapperDlqProducer {

    private static final byte[] INITIAL_ATTEMPT_COUNT = "0".getBytes(StandardCharsets.UTF_8);
    private static final byte[] MAPPER_STAGE = "mapper".getBytes(StandardCharsets.UTF_8);

    private final String mapperDlqEventTopic;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public MapperDlqProducer(KafkaTemplate<String, byte[]> kafkaTemplate,
                             @Value("${kafka.topics.dlq}") String mapperDlqEventTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.mapperDlqEventTopic = mapperDlqEventTopic;
    }

    public CompletableFuture<SendResult<String, byte[]>> send(ConsumerRecord<String, byte[]> sourceRecord, String failureReason) {

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(mapperDlqEventTopic, sourceRecord.key(), sourceRecord.value());

        Header eventId = sourceRecord.headers().lastHeader(EVENT_ID);
        if (eventId != null) {
            record.headers().add(EVENT_ID, eventId.value());
        }

        record.headers()
                .add(SOURCE_TOPIC, sourceRecord.topic().getBytes(StandardCharsets.UTF_8))
                .add(FAILURE_STAGE, MAPPER_STAGE)
                .add(FAILURE_REASON, failureReason.getBytes(StandardCharsets.UTF_8))
                .add(ATTEMPT_COUNT, INITIAL_ATTEMPT_COUNT)
                .add(SOURCE_PARTITION, String.valueOf(sourceRecord.partition()).getBytes(StandardCharsets.UTF_8))
                .add(SOURCE_OFFSET, String.valueOf(sourceRecord.offset()).getBytes(StandardCharsets.UTF_8));

        return kafkaTemplate.send(record);
    }
}
