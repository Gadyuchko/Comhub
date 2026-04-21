package io.comhub.controlplane.config;

import io.comhub.common.config.ConfigCache;
import io.comhub.common.config.ConfigReplayCoordinator;
import io.comhub.common.config.ConfigReplayRebalanceBridge;
import io.comhub.common.config.MappingConfig;
import io.comhub.common.kafka.JsonKafkaDeserializer;
import io.comhub.common.kafka.JsonKafkaSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Spring wiring for the control plane's interaction with {@code comhub.config.v1}.
 *
 * <p>The control plane is the only service that both publishes to and consumes from the
 * configuration topic. HTTP mutations flow out through the {@link KafkaTemplate} declared
 * here, and the same records are replayed back into the local {@link ConfigCache} so that
 * {@code GET /api/source-configs} observes the canonical state the same way every other
 * service in the pipeline does.
 *
 * <p>Declares the per-pod {@link ConfigCache} and {@link ConfigReplayCoordinator} beans,
 * a producer-side pair ({@link ProducerFactory} plus {@link KafkaTemplate}) bound to
 * {@link MappingConfig}, and a consumer-side pair ({@link ConsumerFactory} plus
 * {@link ConcurrentKafkaListenerContainerFactory}) using {@link JsonKafkaDeserializer}
 * against the same type. The control plane does not read source, canonical, or routed
 * topics; its only wire connection is to the configuration topic.
 *
 * <p>{@link EnableKafka} is applied here because Spring Boot 4.x no longer auto-configures
 * Kafka infrastructure; {@code @KafkaListener} processing must be enabled explicitly.
 *
 * @author Roman Hadiuchko
 */
@Configuration
@EnableKafka
public class ControlPlaneKafkaConfiguration {
    @Bean
    public ConfigCache configCache() {
        return new ConfigCache();
    }

    @Bean
    public ConfigReplayCoordinator configReplayCoordinator(ConfigCache configCache) {
        return new ConfigReplayCoordinator(configCache);
    }

    @Bean
    public ProducerFactory<String, MappingConfig> configProducerFactory(
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
    public KafkaTemplate<String, MappingConfig> configKafkaTemplate(ProducerFactory<String, MappingConfig> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
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
}
