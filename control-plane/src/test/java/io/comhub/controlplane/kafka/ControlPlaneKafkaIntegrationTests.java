package io.comhub.controlplane.kafka;

import io.comhub.common.config.ConfigCache;
import io.comhub.controlplane.web.dto.SourceConfigResponse;
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
import org.springframework.web.client.RestTemplate;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end verification that the control-plane HTTP path publishes mutations to
 * {@code comhub.config.v1}, its own listener observes the records, and the local {@link
 * ConfigCache} transitions accordingly. Uses an embedded Kafka broker so the published records
 * travel through real Kafka infrastructure rather than a mocked wire.
 *
 * @author Roman Hadiuchko
 */
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
    ConfigCache cache;

    RestTemplate restTemplate = new RestTemplate();

    @Test
    void postPublishesToConfigTopicAndListenerMaterializesCacheEntry() {
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

        ResponseEntity<SourceConfigResponse> response = restTemplate.postForEntity(
                url("/api/source-configs"),
                new HttpEntity<>(body, headers),
                SourceConfigResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().sourceTopic()).isEqualTo("orders.v1");

        // Listener must observe the published record and materialize it into the local cache.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(cache.get("orders.v1")).isNotNull());

        ResponseEntity<SourceConfigResponse[]> getResponse = restTemplate.getForEntity(
                url("/api/source-configs"),
                SourceConfigResponse[].class);

        assertThat(getResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(getResponse.getBody())
                .extracting(SourceConfigResponse::sourceTopic)
                .contains("orders.v1");
    }

    @Test
    void deletePublishesTombstoneAndListenerRemovesEntryFromCache() {
        String body = """
                {
                  "sourceTopic": "alerts.v1",
                  "displayName": "Alerts",
                  "enabled": true,
                  "rules": [],
                  "emailRecipient": "alerts@example.com"
                }
                """;
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        restTemplate.postForEntity(
                url("/api/source-configs"),
                new HttpEntity<>(body, headers),
                SourceConfigResponse.class);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(cache.get("alerts.v1")).isNotNull());

        restTemplate.delete(url("/api/source-configs/alerts.v1"));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(cache.get("alerts.v1")).isNull());

        ResponseEntity<SourceConfigResponse[]> getResponse = restTemplate.getForEntity(
                url("/api/source-configs"),
                SourceConfigResponse[].class);

        assertThat(getResponse.getBody())
                .extracting(SourceConfigResponse::sourceTopic)
                .doesNotContain("alerts.v1");
    }

    private String url(String path) {
        return "http://localhost:" + port + path;
    }
}
