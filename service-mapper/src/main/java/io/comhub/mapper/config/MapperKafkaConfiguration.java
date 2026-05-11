package io.comhub.mapper.config;

import io.comhub.common.config.ConfigCache;
import io.comhub.common.config.ConfigReplayCoordinator;
import io.comhub.common.config.ConfigReplayRebalanceBridge;
import io.comhub.common.config.MappingConfig;
import io.comhub.common.event.CanonicalEvent;
import io.comhub.common.kafka.JsonKafkaDeserializer;
import io.comhub.common.kafka.JsonKafkaSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Spring wiring for the mapper's Kafka connections. This class only declares beans —
 * it does not run consumers or producers itself.
 *
 * <p>The mapper has two lanes:
 *
 * <p><b>Config lane.</b> Reads {@code MappingConfig} records from {@code comhub.config.v1}
 * and rebuilds local memory. The {@code @KafkaListener} on {@code MappingConfigListener}
 * uses {@link #configKafkaListenerContainerFactory} declared here. If local memory is
 * ever lost, the compacted topic replays it; this lane does not produce records.
 *
 * <p><b>Source-event lane.</b> Every consumed source record must become either a
 * canonical event or a DLQ record. {@link io.comhub.mapper.kafka.SourceListenerManager}
 * builds and starts a {@code ConcurrentMessageListenerContainer} per enabled source
 * topic at runtime — this class only provides the {@link #sourceEventConsumerFactory}
 * the manager uses. The manual-ack semantics live in the manager (it sets
 * {@code AckMode.MANUAL_IMMEDIATE} on the container properties), so a source record is
 * acked only after the canonical or DLQ write is durable.
 *
 * <p>Source records are read as raw bytes. Successful records are parsed and published
 * as {@link CanonicalEvent} via {@link #canonicalKafkaTemplate}; failed records are sent
 * to the DLQ as the original bytes via {@link #dlqKafkaTemplate} so the payload is not
 * changed by JSON parsing or re-serialization.
 *
 * <p>{@link EnableKafka} is applied here because Spring Boot 4.x no longer auto-configures
 * Kafka infrastructure; {@code @KafkaListener} processing must be enabled explicitly. We
 * still need it because the config lane uses {@code @KafkaListener}, even though source
 * listeners are built directly without annotations.
 *
 * @author Roman Hadiuchko
 */
@Configuration
@EnableKafka
public class MapperKafkaConfiguration {

    /* =========  Config event lane ===========*/
    @Bean
    public ConfigCache configCache() {
        return new ConfigCache();
    }

    @Bean
    public ConfigReplayCoordinator configReplayCoordinator(ConfigCache configCache) {
        return new ConfigReplayCoordinator(configCache);
    }

    @Bean
    public ConsumerFactory<String, MappingConfig> configConsumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.consumer.auto-offset-reset:earliest}") String autoOffsetReset) {

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                new JsonKafkaDeserializer<>(MappingConfig.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MappingConfig> configKafkaListenerContainerFactory(
            ConsumerFactory<String, MappingConfig> configConsumerFactory,
            ConfigReplayCoordinator configReplayCoordinator) {

        ConcurrentKafkaListenerContainerFactory<String, MappingConfig> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(configConsumerFactory);
        factory.getContainerProperties().setConsumerRebalanceListener(new ConfigReplayRebalanceBridge(configReplayCoordinator));
        return factory;
    }

    /* =========  Canonical event lane ===========*/

    @Bean
    public ProducerFactory<String, CanonicalEvent> canonicalProducerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.producer.acks:all}") String acks,
            @Value("${spring.kafka.producer.properties.enable.idempotence:true}") Boolean idempotencyEnabled) {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotencyEnabled);

        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), new JsonKafkaSerializer<>());
    }

    @Bean
    public KafkaTemplate<String, CanonicalEvent> canonicalKafkaTemplate(
            ProducerFactory<String, CanonicalEvent> canonicalProducerFactory) {
        return new KafkaTemplate<>(canonicalProducerFactory);
    }

    @Bean
    public ProducerFactory<String, byte[]> dlqProducerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.producer.acks:all}") String acks,
            @Value("${spring.kafka.producer.properties.enable.idempotence:true}") Boolean idempotencyEnabled) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotencyEnabled);

        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), new ByteArraySerializer());
    }

    @Bean
    public KafkaTemplate<String, byte[]> dlqKafkaTemplate(ProducerFactory<String, byte[]> dlqProducerFactory) {
        return new KafkaTemplate<>(dlqProducerFactory);
    }

    @Bean
    public ConsumerFactory<String, byte[]> sourceEventConsumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.consumer.auto-offset-reset:earliest}") String autoOffsetReset) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                new ByteArrayDeserializer());
    }

}
