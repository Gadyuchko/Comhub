package io.comhub.controlplane.web;

import io.comhub.common.config.MappingConfig;
import io.comhub.controlplane.domain.ConfigPublishException;
import io.comhub.controlplane.domain.SourceConfigService;
import io.comhub.controlplane.web.dto.CreateSourceConfigRequest;
import io.comhub.controlplane.web.dto.UpdateSourceConfigRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * MVC-layer tests for {@link SourceConfigsController} and {@link GlobalExceptionHandler}.
 *
 * <p>Covers endpoint shape, validation failures rendered as RFC 7807 {@link
 * org.springframework.http.ProblemDetail} responses with field-local error detail, and the
 * {@code 503} response path when the underlying config publish surfaces as {@link
 * ConfigPublishException}. Kafka behavior is covered separately by integration tests so these
 * tests remain fast and focused on the HTTP contract.
 *
 * @author Roman Hadiuchko
 */
@WebMvcTest(SourceConfigsController.class)
class SourceConfigsControllerTests {

    @Autowired
    MockMvc mockMvc;

    @MockitoBean
    SourceConfigService service;

    @Test
    void getReturnsEmptyListWhenCacheIsEmpty() throws Exception {
        given(service.listAll()).willReturn(List.of());

        mockMvc.perform(get("/api/source-configs"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(0));
    }

    @Test
    void getReturnsAllEntriesFromCache() throws Exception {
        given(service.listAll()).willReturn(List.of(
                new MappingConfig("orders.v1", "Orders", true, 1, List.of(), "ops@example.com"),
                new MappingConfig("alerts.v1", "Alerts", false, 1, List.of(), "alerts@example.com")));

        mockMvc.perform(get("/api/source-configs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].sourceTopic").value("orders.v1"))
                .andExpect(jsonPath("$[1].sourceTopic").value("alerts.v1"))
                .andExpect(jsonPath("$[1].enabled").value(false));
    }

    @Test
    void postCreatesConfigAndReturns201WithResource() throws Exception {
        MappingConfig persisted = new MappingConfig(
                "orders.v1", "Orders", true, 1, List.of(), "ops@example.com");
        given(service.create(any(CreateSourceConfigRequest.class))).willReturn(persisted);

        String body = """
                {
                  "sourceTopic": "orders.v1",
                  "displayName": "Orders",
                  "enabled": true,
                  "rules": [],
                  "emailRecipient": "ops@example.com"
                }
                """;

        mockMvc.perform(post("/api/source-configs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.sourceTopic").value("orders.v1"))
                .andExpect(jsonPath("$.displayName").value("Orders"))
                .andExpect(jsonPath("$.configSchemaVersion").value(1));
    }

    @Test
    void postReturns400ProblemDetailWhenSourceTopicMissing() throws Exception {
        String body = """
                {
                  "displayName": "Orders",
                  "enabled": true,
                  "rules": [],
                  "emailRecipient": "ops@example.com"
                }
                """;

        mockMvc.perform(post("/api/source-configs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.title").value("Invalid request"))
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.fieldErrors.sourceTopic").exists());
    }

    @Test
    void postReturns400ProblemDetailWhenEmailRecipientInvalid() throws Exception {
        String body = """
                {
                  "sourceTopic": "orders.v1",
                  "displayName": "Orders",
                  "enabled": true,
                  "rules": [],
                  "emailRecipient": "not-an-email"
                }
                """;

        mockMvc.perform(post("/api/source-configs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.fieldErrors.emailRecipient").exists());
    }

    @Test
    void postReturns400ProblemDetailWhenRulesMissing() throws Exception {
        String body = """
                {
                  "sourceTopic": "orders.v1",
                  "displayName": "Orders",
                  "enabled": true,
                  "emailRecipient": "ops@example.com"
                }
                """;

        mockMvc.perform(post("/api/source-configs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.fieldErrors.rules").exists());
    }

    @Test
    void postReturns503ProblemDetailWhenPublishFails() throws Exception {
        willThrow(new ConfigPublishException("publish timed out after 5s", new RuntimeException()))
                .given(service).create(any(CreateSourceConfigRequest.class));

        String body = """
                {
                  "sourceTopic": "orders.v1",
                  "displayName": "Orders",
                  "enabled": true,
                  "rules": [],
                  "emailRecipient": "ops@example.com"
                }
                """;

        mockMvc.perform(post("/api/source-configs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isServiceUnavailable())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.title").value("Config topic unavailable"))
                .andExpect(jsonPath("$.status").value(503));
    }

    @Test
    void putUpdatesConfigAndReturns200WithResource() throws Exception {
        MappingConfig updated = new MappingConfig(
                "orders.v1", "Orders v2", false, 1, List.of(), "ops@example.com");
        given(service.update(eq("orders.v1"), any(UpdateSourceConfigRequest.class)))
                .willReturn(updated);

        String body = """
                {
                  "displayName": "Orders v2",
                  "enabled": false,
                  "rules": [],
                  "emailRecipient": "ops@example.com"
                }
                """;

        mockMvc.perform(put("/api/source-configs/orders.v1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sourceTopic").value("orders.v1"))
                .andExpect(jsonPath("$.displayName").value("Orders v2"))
                .andExpect(jsonPath("$.enabled").value(false));
    }

    @Test
    void deleteReturns204AndDelegatesToService() throws Exception {
        mockMvc.perform(delete("/api/source-configs/orders.v1"))
                .andExpect(status().isNoContent());

        verify(service).delete("orders.v1");
    }

    @Test
    void deleteReturns503WhenTombstonePublishFails() throws Exception {
        willThrow(new ConfigPublishException("publish timed out", new RuntimeException()))
                .given(service).delete("orders.v1");

        mockMvc.perform(delete("/api/source-configs/orders.v1"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value(503));
    }
}
