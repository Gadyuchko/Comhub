package io.comhub.mapper.kafka;

import io.comhub.common.event.CanonicalEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static io.comhub.common.event.EventHeaders.*;

/**
 * Publishes mapped canonical events with the Kafka headers needed for traceability.
 *
 * @author Roman Hadiuchko
 */
@Component
public class CanonicalEventProducer {

    private static final byte[] INITIAL_ATTEMPT_COUNT = "0".getBytes(StandardCharsets.UTF_8);

    private final String canonicalEventTopic;
    private final KafkaTemplate<String, CanonicalEvent> kafkaTemplate;

    public CanonicalEventProducer(KafkaTemplate<String, CanonicalEvent> kafkaTemplate,
                                  @Value("${kafka.topics.canonical}") String canonicalEventTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.canonicalEventTopic = canonicalEventTopic;
    }

    public CompletableFuture<SendResult<String, CanonicalEvent>> send(CanonicalEvent event) {

        ProducerRecord<String, CanonicalEvent> record = new ProducerRecord<>(canonicalEventTopic, event.id().toString(), event);

        record.headers()
                .add(EVENT_ID, event.id().toString().getBytes(StandardCharsets.UTF_8))
                .add(SOURCE_TOPIC, event.sourceTopic().getBytes(StandardCharsets.UTF_8))
                .add(SOURCE_EVENT_TYPE, event.sourceEventType().getBytes(StandardCharsets.UTF_8))
                .add(SOURCE_PARTITION, event.sourcePartition().toString().getBytes(StandardCharsets.UTF_8))
                .add(SOURCE_OFFSET, event.sourceOffset().toString().getBytes(StandardCharsets.UTF_8))
                .add(ATTEMPT_COUNT, INITIAL_ATTEMPT_COUNT);

        return kafkaTemplate.send(record);
    }
}
