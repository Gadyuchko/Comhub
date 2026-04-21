package io.comhub.controlplane.kafka;

import io.comhub.common.config.ConfigCache;
import io.comhub.common.config.MappingConfig;
import io.comhub.controlplane.domain.ConfigPublishException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.RestTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.willThrow;

/**
 * Verifies that when the config-topic publish path fails, the control-plane HTTP surface
 * returns {@code 503 Service Unavailable} and the local {@link ConfigCache} is left untouched.
 *
 * <p>The publisher bean is replaced with a Mockito stub that throws {@link
 * ConfigPublishException} unconditionally. The embedded Kafka broker is still started so the
 * Spring context, listener, and startup orchestrator boot normally; only the outbound publish
 * path is broken.
 *
 * @author Roman Hadiuchko
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1, topics = ControlPlanePublishFailureIntegrationTests.CONFIG_TOPIC)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "kafka.topics.config=" + ControlPlanePublishFailureIntegrationTests.CONFIG_TOPIC,
        "kafka.group.config-replay=test-control-plane-publish-failure",
        "kafka.config-replay.startup-timeout=20s",
        "kafka.publisher.timeout=5s"
})
class ControlPlanePublishFailureIntegrationTests {

    static final String CONFIG_TOPIC = "comhub.config.v1";

    @LocalServerPort
    int port;

    @Autowired
    ConfigCache cache;

    @MockitoBean
    ConfigTopicPublisher publisher;

    RestTemplate restTemplate = new RestTemplate();

    ControlPlanePublishFailureIntegrationTests() {
        restTemplate.setErrorHandler(new DefaultResponseErrorHandler() {
            @Override
            public boolean hasError(org.springframework.http.client.ClientHttpResponse response) {
                return false;
            }
        });
    }

    @Test
    void postReturns503AndCacheIsUnchangedWhenPublishFails() {
        willThrow(new ConfigPublishException("simulated broker failure", new RuntimeException()))
                .given(publisher).publish(any(MappingConfig.class));

        int sizeBefore = cache.size();

        String body = """
                {
                  "sourceTopic": "orders.v1",
                  "displayName": "Orders",
                  "enabled": true,
                  "rules": [],
                  "emailRecipient": "ops@example.com"
                }
                """;
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        ResponseEntity<String> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/api/source-configs",
                new HttpEntity<>(body, headers),
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getHeaders().getContentType())
                .isNotNull()
                .satisfies(ct -> assertThat(ct.includes(MediaType.APPLICATION_PROBLEM_JSON)).isTrue());
        assertThat(response.getBody()).contains("Config topic unavailable");

        assertThat(cache.size()).isEqualTo(sizeBefore);
        assertThat(cache.get("orders.v1")).isNull();
    }

    @Test
    void deleteReturns503AndCacheIsUnchangedWhenTombstonePublishFails() {
        willThrow(new ConfigPublishException("simulated broker failure", new RuntimeException()))
                .given(publisher).publishTombstone("orders.v1");

        int sizeBefore = cache.size();

        ResponseEntity<String> response = restTemplate.exchange(
                "http://localhost:" + port + "/api/source-configs/orders.v1",
                org.springframework.http.HttpMethod.DELETE,
                null,
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(cache.size()).isEqualTo(sizeBefore);
    }
}
