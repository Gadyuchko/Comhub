package io.comhub.mapper.kafka;

import io.comhub.common.config.ConfigCache;
import io.comhub.common.config.ConfigKey;
import io.comhub.common.config.MappingConfig;
import io.comhub.common.event.CanonicalEvent;
import io.comhub.mapper.domain.MappingEngine;
import io.comhub.mapper.domain.MappingFailureException;
import io.comhub.mapper.domain.PreparedSourceEvent;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Owns one Kafka consumer per enabled source topic and runs them at runtime.
 *
 * <p>Normally with {@code @KafkaListener} Spring builds listener containers at startup
 * from annotations. That doesn't work here — source topics aren't known at build time,
 * they come from the config cache:
 *
 * <p>{@code orders.events.v1}
 * <br>{@code payments.events.v1}
 * <br>{@code legacy.events.v1}
 *
 * <p>So we build {@link ConcurrentMessageListenerContainer}s ourselves. For each topic
 * with at least one enabled config we construct a container, set its group id and
 * manual ack mode, attach an {@link AcknowledgingMessageListener} that delegates to
 * {@link #handle(ConsumerRecord, Acknowledgment)}, and {@code start()} it. We track
 * the container in {@link #activeContainers} so we can stop it later when the last
 * config for its topic is removed or disabled.
 *
 * <p>Per-topic locks in {@link #topicLocks} make sure only one thread starts or stops
 * the container for the same topic at a time. Different topics reconcile in parallel.
 *
 * <p>{@link #shutdown()} stops every container when the application is going down so
 * we never leave consumer threads running after Spring closes the context.
 *
 * @author Roman Hadiuchko
 */
@Component
public class SourceListenerManager {

    private static final Logger log = LoggerFactory.getLogger(SourceListenerManager.class);

    private final ConcurrentHashMap<String, ConcurrentMessageListenerContainer<String, byte[]>> activeContainers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> topicLocks = new ConcurrentHashMap<>();
    private final ConfigCache cache;
    private final ConsumerFactory<String, byte[]> sourceEventConsumerFactory;
    private final MapperDlqProducer dlqProducer;
    private final MappingEngine mappingEngine;
    private final CanonicalEventProducer canonicalEventProducer;

    public SourceListenerManager(
            ConfigCache cache,
            ConsumerFactory<String, byte[]> sourceEventConsumerFactory,
            MapperDlqProducer mapperDlqProducer,
            MappingEngine mappingEngine,
            CanonicalEventProducer canonicalEventProducer) {
        this.cache = cache;
        this.sourceEventConsumerFactory = sourceEventConsumerFactory;
        this.dlqProducer = mapperDlqProducer;
        this.mappingEngine = mappingEngine;
        this.canonicalEventProducer = canonicalEventProducer;
    }

    /* ============================================================================== */
    /* ============= SOURCE EVENTS CONTAINER LISTERS LIFECYCLE METHODS ============== */
    /* ============================================================================== */

    /**
     * Starts listening to the topic if it's enabled in config cache and not already listening.
     * Only one thread at a time may start/stop listener for the same topic.
     * We get or create lock for specific topic and start/stop listener if needed.
     */
    public void reconcile(String topic) {
        Object lock = topicLocks.computeIfAbsent(topic, ignored -> new Object());
        synchronized (lock) {
            boolean isEnabled = enabledTopic(topic);
            boolean isAlreadyListening = activeContainers.containsKey(topic);

            if (isEnabled && !isAlreadyListening) {
                startContainer(topic);
            } else if (isAlreadyListening && !isEnabled) {
                stopContainer(topic);
            }
        }
    }

    private void stopContainer(String topic) {
        ConcurrentMessageListenerContainer<String, byte[]> container = activeContainers.remove(topic);
        if (container == null) {
            return;
        }
        container.stop();
        log.info("Stopped source listener topic={}", topic);
    }

    /**
     * Builds and starts a Kafka consumer for the given topic.
     *
     * <p>{@link ContainerProperties} carries everything per-topic: which topic to subscribe
     * to, the consumer group id, the ack mode, and the listener callback that runs on
     * every record.
     *
     * <p>Group id is per topic ({@code comhub.mapper.<sanitized-topic>}). One group per
     * topic gives us independent lag tracking in Kafka and lets KEDA scale this listener
     * based on that group's lag in a later story.
     *
     * <p>Manual ack mode means the consumer commits offsets only after we explicitly call
     * {@code ack.acknowledge()} from {@link #handle(ConsumerRecord, Acknowledgment)}. We
     * call it after the canonical (or DLQ) write is acked by Kafka, so a source record is
     * never marked as handled before its result is durable.
     *
     * <p>Topic name is used verbatim for the subscription. The sanitized form is only used
     * for the group id and the bean name — we never subscribe to the sanitized name or we
     * would subscribe to the wrong topic.
     */
    private void startContainer(String topic) {
        String groupId = buildGroupIdFromTopic(topic);

        ContainerProperties properties = new ContainerProperties(topic);
        properties.setGroupId(groupId);
        properties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        properties.setMessageListener(
                (AcknowledgingMessageListener<String, byte[]>) (record, ack) -> handle(record, ack));

        ConcurrentMessageListenerContainer<String, byte[]> container =
                new ConcurrentMessageListenerContainer<>(sourceEventConsumerFactory, properties);
        container.setBeanName(buildIdFromTopic(topic));   // shows up in container logs

        container.start();
        activeContainers.put(topic, container);
        log.info("Started source listener topic={} groupId={}", topic, groupId);
    }

    private String buildGroupIdFromTopic(String topic) {
        return "comhub.mapper.".concat(sanitize(topic));
    }

    private String buildIdFromTopic(String topic) {
        return "source::".concat(sanitize(topic));
    }

    public void reconcileAll() {
        enabledTopics().forEach(this::reconcile);
    }

    /**
     * Returns {@code true} if a container is currently running for the given topic.
     * Mainly used by integration tests to wait for dynamic registration / removal.
     */
    public boolean hasListenerFor(String topic) {
        return activeContainers.containsKey(topic);
    }

    private Set<String> enabledTopics() {
        return cache.values().stream()
                .filter(MappingConfig::enabled)
                .map(MappingConfig::topic)
                .collect(HashSet::new, Set::add, Set::addAll);
    }

    private boolean enabledTopic(String topic) {
        return cache.configsForTopic(topic).stream().anyMatch(MappingConfig::enabled);
    }

    private String sanitize(String topic) {
        return topic.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    /* ============================================================================== */
    /* ========================== LISTENER HANDING METHODS ========================== */
    /* ============================================================================== */

    /**
     * This method is back bone of whole system, this is where source events are processed, mapped and canonical events are produced.
     * Since we enforce unique discriminator per topic (TOPIC, HEADER, PAYLOAD) we can safely extract discriminator from any config.
     * And use it to prepare source event and then look up config in cache.
     * Which gives us all the info we need to process the event with O(1) complexity.
     *
     * @param record
     * @param acknowledgment
     */
    private void handle(ConsumerRecord<String, byte[]> record, Acknowledgment acknowledgment) {
        String topic = record.topic();
        Collection<MappingConfig> topicConfigs = cache.configsForTopic(topic);

        if (topicConfigs.isEmpty()) {
            // skip empty topics, they are not enabled
            acknowledgment.acknowledge();
            return;
        }

        // any config works we only need this one to extract discriminator,
        // validation guarantees identical discriminators across the topic
        MappingConfig anyConfig = topicConfigs.iterator().next();

        // prepare source event that we will use to seek config, this will throw if config is invalid
        PreparedSourceEvent prepared;
        try {
            prepared = mappingEngine.prepare(anyConfig, record);
        } catch (MappingFailureException e) {
            dlqProducer.send(record, e.reason()).join();
            acknowledgment.acknowledge();
            return;
        }
        // and here we actually check if such config exists and enabled, skip if not since we dont need to process it
        MappingConfig matched = cache.get(new ConfigKey(topic, prepared.sourceEventType()));
        if (matched == null || !matched.enabled()) {
            acknowledgment.acknowledge();
            return;
        }

        CanonicalEvent canonicalEvent;
        try{
           canonicalEvent = mappingEngine.map(matched, prepared);
        } catch (MappingFailureException e) {
            dlqProducer.send(record, e.reason()).join();
            acknowledgment.acknowledge();
            return;
        }

        canonicalEventProducer.send(canonicalEvent).join();
        acknowledgment.acknowledge();
    }

    @PreDestroy
    public void shutdown() {
        activeContainers.values().forEach(ConcurrentMessageListenerContainer::stop);
        activeContainers.clear();
        log.info("Stopped all source listener containers on shutdown");
    }

}
