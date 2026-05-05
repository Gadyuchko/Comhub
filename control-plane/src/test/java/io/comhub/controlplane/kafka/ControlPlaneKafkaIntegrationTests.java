package io.comhub.controlplane.kafka;

import io.comhub.common.config.ConfigKey;
import io.comhub.common.config.MappingConfig;
import io.comhub.common.kafka.JsonKafkaDeserializer;
import io.comhub.controlplane.web.dto.SourceConfigResponse;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1, topics = ControlPlaneKafkaIntegrationTests.CONFIG_TOPIC)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "kafka.topics.config=" + ControlPlaneKafkaIntegrationTests.CONFIG_TOPIC,
        "kafka.group.config-replay=test-control-plane-round-trip",
        "kafka.config-replay.startup-timeout=20s",
        "kafka.publisher.timeout=5s"
})
class ControlPlaneKafkaIntegrationTests {

    static final String CONFIG_TOPIC = "comhub.config.v1";

    @LocalServerPort
    int port;

    @Autowired
    io.comhub.common.config.ConfigCache cache;

    @Autowired
    org.springframework.kafka.test.EmbeddedKafkaBroker broker;

    RestTemplate restTemplate = new RestTemplate();
    Consumer<String, MappingConfig> auditConsumer;

    @AfterEach
    void tearDown() {
        if (auditConsumer != null) {
            auditConsumer.close();
        }
    }

    @Test
    void postPublishesCompositeKeyAndListenerMaterializesCacheEntry() {
        ResponseEntity<SourceConfigResponse> response = restTemplate.postForEntity(
                url("/api/source-configs"),
                new HttpEntity<>(validBody("orders.v1", "order-created"), jsonHeaders()),
                SourceConfigResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().topic()).isEqualTo("orders.v1");
        assertThat(response.getBody().sourceEventType()).isEqualTo("order-created");

        ConfigKey key = new ConfigKey("orders.v1", "order-created");
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(cache.get(key)).isNotNull());

        ConsumerRecord<String, MappingConfig> record = kafkaRecord();
        assertThat(record.key()).isEqualTo(key.asRecordKey());
        assertThat(record.value().topic()).isEqualTo("orders.v1");
        assertThat(record.value().sourceEventType()).isEqualTo("order-created");

        ResponseEntity<SourceConfigResponse[]> getResponse = restTemplate.getForEntity(
                url("/api/source-configs"),
                SourceConfigResponse[].class);

        assertThat(getResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(getResponse.getBody())
                .extracting(SourceConfigResponse::topic)
                .contains("orders.v1");
    }

    @Test
    void twoPeerConfigsRoundTripAndDeletingOnePublishesPeerPreservingTombstone() {
        restTemplate.postForEntity(
                url("/api/source-configs"),
                new HttpEntity<>(validBody("orders.v1", "order-created"), jsonHeaders()),
                SourceConfigResponse.class);
        restTemplate.postForEntity(
                url("/api/source-configs"),
                new HttpEntity<>(validBody("orders.v1", "order-cancelled"), jsonHeaders()),
                SourceConfigResponse.class);

        ConfigKey created = new ConfigKey("orders.v1", "order-created");
        ConfigKey cancelled = new ConfigKey("orders.v1", "order-cancelled");
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(cache.get(created)).isNotNull();
            assertThat(cache.get(cancelled)).isNotNull();
        });

        ResponseEntity<Void> deleteResponse = restTemplate.exchange(
                url("/api/source-configs/orders.v1/order-cancelled"),
                HttpMethod.DELETE,
                null,
                Void.class);

        assertThat(deleteResponse.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(cache.get(created)).isNotNull();
            assertThat(cache.get(cancelled)).isNull();
        });

        ResponseEntity<SourceConfigResponse[]> getResponse = restTemplate.getForEntity(
                url("/api/source-configs"),
                SourceConfigResponse[].class);

        assertThat(getResponse.getBody())
                .extracting(SourceConfigResponse::sourceEventType)
                .contains("order-created")
                .doesNotContain("order-cancelled");
    }

    private ConsumerRecord<String, MappingConfig> kafkaRecord() {
        if (auditConsumer == null) {
            Map<String, Object> props = KafkaTestUtils.consumerProps(
                    "audit-" + UUID.randomUUID(), "false", broker);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            auditConsumer = new DefaultKafkaConsumerFactory<>(
                    props,
                    new StringDeserializer(),
                    new JsonKafkaDeserializer<>(MappingConfig.class))
                    .createConsumer();
            broker.consumeFromAnEmbeddedTopic(auditConsumer, CONFIG_TOPIC);
        }
        return KafkaTestUtils.getSingleRecord(auditConsumer, CONFIG_TOPIC, Duration.ofSeconds(10));
    }

    private HttpHeaders jsonHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return headers;
    }

    private String validBody(String topic, String sourceEventType) {
        return """
                {
                  "topic": "%s",
                  "sourceEventType": "%s",
                  "enabled": true,
                  "discriminator": {
                    "source": "header",
                    "key": "eventType"
                  },
                  "mapping": {
                    "occurredAt": {
                      "source": "/occurredAt"
                    },
                    "severity": {
                      "source": "/severity"
                    },
                    "category": {
                      "source": "/category"
                    },
                    "subject": {
                      "source": "/subject"
                    },
                    "message": {
                      "source": "/message"
                    },
                    "attributes": []
                  },
                  "operations": {
                    "promotedAttributes": [],
                    "classification": [],
                    "routing": []
                  }
                }
                """.formatted(topic, sourceEventType);
    }

    private String url(String path) {
        return "http://localhost:" + port + path;
    }
}
