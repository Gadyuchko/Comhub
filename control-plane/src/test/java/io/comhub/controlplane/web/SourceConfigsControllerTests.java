package io.comhub.controlplane.web;

import io.comhub.common.config.CanonicalFieldMapping;
import io.comhub.common.config.CanonicalMapping;
import io.comhub.common.config.ClassificationRule;
import io.comhub.common.config.Condition;
import io.comhub.common.config.ConfigDiscriminator;
import io.comhub.common.config.DiscriminatorSource;
import io.comhub.common.config.MappingConfig;
import io.comhub.common.config.OperationsConfig;
import io.comhub.common.config.PromotedAttribute;
import io.comhub.common.config.RoutingAction;
import io.comhub.common.config.RoutingRule;
import io.comhub.controlplane.domain.ConfigPublishException;
import io.comhub.controlplane.domain.InvalidSourceConfigException;
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
                sampleConfig("orders.v1", "order-created"),
                sampleConfig("alerts.v1", "alert-created")));

        mockMvc.perform(get("/api/source-configs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].topic").value("orders.v1"))
                .andExpect(jsonPath("$[0].sourceEventType").value("order-created"))
                .andExpect(jsonPath("$[1].topic").value("alerts.v1"));
    }

    @Test
    void postCreatesConfigAndReturns201WithResource() throws Exception {
        given(service.create(any(CreateSourceConfigRequest.class))).willReturn(sampleConfig("orders.v1", "order-created"));

        mockMvc.perform(post("/api/source-configs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validBody("orders.v1", "order-created")))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.topic").value("orders.v1"))
                .andExpect(jsonPath("$.sourceEventType").value("order-created"))
                .andExpect(jsonPath("$.configSchemaVersion").value(2))
                .andExpect(jsonPath("$.discriminator.source").value("header"))
                .andExpect(jsonPath("$.operations.routing[0].actions[0].target").value("ops@example.com"));
    }

    @Test
    void postReturns400ProblemDetailWhenTopicMissing() throws Exception {
        String body = validBody("orders.v1", "order-created").replace("\"topic\": \"orders.v1\",", "");
        willThrow(new InvalidSourceConfigException(java.util.Map.of("topic", "must not be blank")))
                .given(service).create(any(CreateSourceConfigRequest.class));

        mockMvc.perform(post("/api/source-configs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.title").value("Invalid request"))
                .andExpect(jsonPath("$.fieldErrors.topic").exists());
    }

    @Test
    void postReturns400ProblemDetailWhenNestedDiscriminatorKeyMissing() throws Exception {
        String body = """
                {
                  "topic": "orders.v1",
                  "sourceEventType": "order-created",
                  "enabled": true,
                  "discriminator": {
                    "source": "header",
                    "key": ""
                  },
                  "mapping": {
                    "attributes": []
                  },
                  "operations": {
                    "promotedAttributes": [],
                    "classification": [],
                    "routing": []
                  }
                }
                """;
        willThrow(new InvalidSourceConfigException(java.util.Map.of("discriminator.key", "must not be blank")))
                .given(service).create(any(CreateSourceConfigRequest.class));

        mockMvc.perform(post("/api/source-configs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.fieldErrors['discriminator.key']").exists());
    }

    @Test
    void postReturns400ProblemDetailWhenDiscriminatorSourceUnknown() throws Exception {
        String body = """
                {
                  "topic": "orders.v1",
                  "sourceEventType": "order-created",
                  "enabled": true,
                  "discriminator": {
                    "source": "unknown",
                    "key": "eventType"
                  },
                  "mapping": {
                    "attributes": []
                  },
                  "operations": {
                    "promotedAttributes": [],
                    "classification": [],
                    "routing": []
                  }
                }
                """;
        mockMvc.perform(post("/api/source-configs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("Malformed request"));
    }

    @Test
    void postReturns400ProblemDetailWhenOperationsMissing() throws Exception {
        String body = """
                {
                  "topic": "orders.v1",
                  "sourceEventType": "order-created",
                  "enabled": true,
                  "discriminator": {
                    "source": "header",
                    "key": "eventType"
                  },
                  "mapping": {
                    "attributes": []
                  }
                }
                """;
        willThrow(new InvalidSourceConfigException(java.util.Map.of("operations", "must not be null")))
                .given(service).create(any(CreateSourceConfigRequest.class));

        mockMvc.perform(post("/api/source-configs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.fieldErrors.operations").exists());
    }

    @Test
    void postReturns503ProblemDetailWhenPublishFails() throws Exception {
        willThrow(new ConfigPublishException("publish timed out after 5s", new RuntimeException()))
                .given(service).create(any(CreateSourceConfigRequest.class));

        mockMvc.perform(post("/api/source-configs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validBody("orders.v1", "order-created")))
                .andExpect(status().isServiceUnavailable())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.title").value("Config topic unavailable"))
                .andExpect(jsonPath("$.status").value(503));
    }

    @Test
    void putUpdatesConfigAndReturns200WithResource() throws Exception {
        given(service.update(eq("orders.v1"), eq("order-created"), any(UpdateSourceConfigRequest.class)))
                .willReturn(sampleConfig("orders.v1", "order-created"));

        mockMvc.perform(put("/api/source-configs/orders.v1/order-created")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validBody("orders.v1", "order-created")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.topic").value("orders.v1"))
                .andExpect(jsonPath("$.sourceEventType").value("order-created"));
    }

    @Test
    void putReturns400WhenPathAndBodyIdentityMismatch() throws Exception {
        willThrow(new InvalidSourceConfigException(java.util.Map.of(
                "topic", "must match path topic",
                "sourceEventType", "must match path sourceEventType")))
                .given(service).update(eq("orders.v1"), eq("order-created"), any(UpdateSourceConfigRequest.class));

        mockMvc.perform(put("/api/source-configs/orders.v1/order-created")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validBody("orders.v1", "order-updated")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.fieldErrors.topic").value("must match path topic"))
                .andExpect(jsonPath("$.fieldErrors.sourceEventType").value("must match path sourceEventType"));
    }

    @Test
    void deleteReturns204AndDelegatesToService() throws Exception {
        mockMvc.perform(delete("/api/source-configs/orders.v1/order-created"))
                .andExpect(status().isNoContent());

        verify(service).delete("orders.v1", "order-created");
    }

    @Test
    void deleteReturns503WhenTombstonePublishFails() throws Exception {
        willThrow(new ConfigPublishException("publish timed out", new RuntimeException()))
                .given(service).delete("orders.v1", "order-created");

        mockMvc.perform(delete("/api/source-configs/orders.v1/order-created"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value(503));
    }

    private MappingConfig sampleConfig(String topic, String sourceEventType) {
        return new MappingConfig(
                topic,
                sourceEventType,
                true,
                2,
                new ConfigDiscriminator(DiscriminatorSource.HEADER, "eventType"),
                new CanonicalMapping(
                        new CanonicalFieldMapping("/occurredAt"),
                        new CanonicalFieldMapping("/severity"),
                        new CanonicalFieldMapping("/category"),
                        new CanonicalFieldMapping("/subject"),
                        new CanonicalFieldMapping("/message"),
                        List.of()),
                new OperationsConfig(
                        List.of(new PromotedAttribute("customerId", "customerId")),
                        List.of(new ClassificationRule(
                                "ORDERS",
                                "order-handler",
                                List.of(new Condition("severity", "eq", "ERROR")))),
                        List.of(new RoutingRule(
                                "primary-email",
                                List.of(new Condition("classificationCode", "eq", "ORDERS")),
                                List.of(new RoutingAction("notify", "email", "ops@example.com"))))));
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
                    "attributes": [
                      {
                        "targetAttribute": "customerId",
                        "source": "/customerId"
                      }
                    ]
                  },
                  "operations": {
                    "promotedAttributes": [
                      {
                        "sourceAttribute": "customerId",
                        "targetAttribute": "customerId"
                      }
                    ],
                    "classification": [
                      {
                        "code": "ORDERS",
                        "handler": "order-handler",
                        "conditions": [
                          {
                            "attribute": "severity",
                            "operator": "eq",
                            "value": "ERROR"
                          }
                        ]
                      }
                    ],
                    "routing": [
                      {
                        "handler": "primary-email",
                        "conditions": [
                          {
                            "attribute": "classificationCode",
                            "operator": "eq",
                            "value": "ORDERS"
                          }
                        ],
                        "actions": [
                          {
                            "type": "notify",
                            "channel": "email",
                            "target": "ops@example.com"
                          }
                        ]
                      }
                    ]
                  }
                }
                """.formatted(topic, sourceEventType);
    }
}
