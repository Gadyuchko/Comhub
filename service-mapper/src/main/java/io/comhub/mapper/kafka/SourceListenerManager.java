package io.comhub.mapper.kafka;

import io.comhub.common.config.ConfigCache;
import io.comhub.common.config.MappingConfig;
import io.comhub.common.event.CanonicalEvent;
import io.comhub.mapper.domain.MappingEngine;
import io.comhub.mapper.domain.MappingFailureException;
import io.comhub.mapper.domain.PreparedSourceEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the lifecycle of the {@code @KafkaListener}s for the source events.
 * Normally, with {@code @KafkaListener}, Spring creates listener containers at startup and manages them for you.
 *
 * <p>But for this manager, source topics are dynamic and come from config cache:
 *
 * <p>{@code orders.events.v1}
 * <br>{@code payments.events.v1}
 * <br>{@code legacy.events.v1}
 *
 * <p>So we have to create listeners at runtime. {@link KafkaListenerEndpointRegistry}
 * gives us a way to do it. And we use predefined {@link ConcurrentKafkaListenerContainerFactory}
 * to create the custom listener container for source events.
 *
 * <p>Factory = template how to create listener containers.
 * <br>Registry = bridge to operate listener containers: register, start, stop, unregister.
 *
 * @author Roman Hadiuchko
 */
@Component
public class SourceListenerManager {

    private static final Logger log = LoggerFactory.getLogger(SourceListenerManager.class);

    private final ConcurrentHashMap<String, String> activeListeners = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> topicLocks = new ConcurrentHashMap<>();
    private final ConfigCache cache;
    // This comes from Spring Kafka infrastructure.
    private final KafkaListenerEndpointRegistry registry;
    // This is our defined bean for source event listeners.
    private final ConcurrentKafkaListenerContainerFactory<String, byte[]> sourceEventKafkaListenerContainerFactory;
    private final MapperDlqProducer dlqProducer;
    private final MappingEngine mappingEngine;
    private final CanonicalEventProducer canonicalEventProducer;

    public SourceListenerManager(
            ConfigCache cache,
            KafkaListenerEndpointRegistry registry,
            @Qualifier("sourceEventKafkaListenerContainerFactory")
            ConcurrentKafkaListenerContainerFactory<String, byte[]> sourceEventKafkaListenerContainerFactory,
            MapperDlqProducer mapperDlqProducer,
            MappingEngine mappingEngine,
            CanonicalEventProducer canonicalEventProducer) {
        this.cache = cache;
        this.registry = registry;
        this.sourceEventKafkaListenerContainerFactory = sourceEventKafkaListenerContainerFactory;
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
            boolean isAlreadyListening = activeListeners.containsKey(topic);

            if (isEnabled && !isAlreadyListening) {
                startListener(topic);
            } else if (isAlreadyListening && !isEnabled) {
                stopListener(topic);
            }
        }
    }

    private void stopListener(String topic) {
        String endpointId = activeListeners.remove(topic);
        if (endpointId == null) {
            return;
        }

        MessageListenerContainer container = registry.getListenerContainer(endpointId);
        if (container != null) {
            container.stop();
        }
        registry.unregisterListenerContainer(endpointId);
    }

    /**
     * We delegate container build and start to Spring registry and only build Endpoint.
     * We need {@code id} for referencing later from registry to start, stop, and unregister listener container.
     * {@code groupId} Kafka tracks offsets per consumer group. Per-topic group id gives independent lag visibility, KEDA can later scale based on that group's lag.
     * {@code topics} is the real Kafka topic name. We do not sanitize this one, otherwise we may subscribe to the wrong topic.
     * {@code bean} is the object that has listener method. In our case, the bean is current SourceListenerManager instance.
     */
    private void startListener(String topic) {
        MethodKafkaListenerEndpoint<String, byte[]> endpoint = new MethodKafkaListenerEndpoint<>();
        endpoint.setId(buildIdFromTopic(topic));
        endpoint.setGroupId(buildGroupIdFromTopic(topic));
        endpoint.setTopics(topic);
        endpoint.setBean(this);
        endpoint.setMethod(handleMethod());

        registry.registerListenerContainer(endpoint, sourceEventKafkaListenerContainerFactory, true);
        activeListeners.put(topic, endpoint.getId());
        log.info("Started source listener topic={} endpointId={} groupId={}", topic, endpoint.getId(), endpoint.getGroupId());
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


    public void handle(ConsumerRecord<String, byte[]> record, Acknowledgment acknowledgment) {
        String topic = record.topic();

        if (!enabledTopic(topic)) {
            dlqProducer.send(record, "no_config_found").join();
            acknowledgment.acknowledge();
            return;
        }

        boolean hadExtractionFailure = false;
        boolean hadExtractedValue = false;

        for (MappingConfig config : cache.configsForTopic(topic)) {
            if (!config.enabled()) {
                continue;
            }

            try {
                PreparedSourceEvent sourceEvent = mappingEngine.prepare(config, record);
                hadExtractedValue = true;

                if (sourceEvent.sourceEventType().equals(config.sourceEventType())) {
                    CanonicalEvent canonicalEvent = mappingEngine.map(config, sourceEvent);
                    canonicalEventProducer.send(canonicalEvent).join();
                    acknowledgment.acknowledge();
                    return;
                }
            } catch (MappingFailureException e) {
                hadExtractionFailure = true;
            }
        }

        String reason = hadExtractedValue || !hadExtractionFailure
                ? "no_config_found"
                : "discriminator_extraction_failed";
        dlqProducer.send(record, reason).join();
        acknowledgment.acknowledge();
    }

    private Method handleMethod() {
        try {
            return SourceListenerManager.class.getMethod("handle", ConsumerRecord.class, Acknowledgment.class);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Source listener handle method is missing", e);
        }
    }

}
