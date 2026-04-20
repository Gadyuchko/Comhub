package io.comhub.mapper.config;

import io.comhub.common.config.ConfigCache;
import io.comhub.common.config.ConfigReplayCoordinator;
import io.comhub.common.config.MappingConfig;
import io.comhub.common.kafka.JsonKafkaDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Spring wiring for the mapper service's consumption of {@code comhub.config.v1}.
 *
 * <p>Declares the shared {@link ConfigCache} and {@link ConfigReplayCoordinator}
 * beans used by the config listener, plus a dedicated {@link ConsumerFactory}
 * and {@link ConcurrentKafkaListenerContainerFactory} configured with
 * {@link JsonKafkaDeserializer} bound to {@link MappingConfig}. The container
 * factory is named explicitly so the config listener references it by name
 * and future source-topic factories can coexist without collision.
 *
 * <p>{@link EnableKafka} is applied here because Spring Boot 4.x no longer
 * auto-configures Kafka infrastructure; {@code @KafkaListener} processing
 * must be enabled explicitly.
 *
 * @author Roman Hadiuchko
 */
@Configuration
@EnableKafka
public class MapperKafkaConfiguration {

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
            ConsumerFactory<String, MappingConfig> configConsumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, MappingConfig> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(configConsumerFactory);
        return factory;
    }
}
