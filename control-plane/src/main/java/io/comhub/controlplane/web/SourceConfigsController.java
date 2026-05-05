package io.comhub.controlplane.web;

import io.comhub.common.config.MappingConfig;
import io.comhub.controlplane.domain.SourceConfigService;
import io.comhub.controlplane.web.dto.CreateSourceConfigRequest;
import io.comhub.controlplane.web.dto.SourceConfigResponse;
import io.comhub.controlplane.web.dto.UpdateSourceConfigRequest;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * HTTP surface for source-configuration management. Reads are served from the control-plane's
 * local {@code ConfigCache}; mutations are published to {@code comhub.config.v1} and only become
 * visible to subsequent reads after the config listener observes them.
 *
 * @author Roman Hadiuchko
 */
@RestController
@RequestMapping("/api/source-configs")
public class SourceConfigsController {

    private final SourceConfigService service;

    public SourceConfigsController(SourceConfigService service) {
        this.service = service;
    }

    @GetMapping
    public List<SourceConfigResponse> list() {
        return service.listAll().stream()
                .map(SourceConfigResponse::from)
                .toList();
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public SourceConfigResponse create(@Valid @RequestBody CreateSourceConfigRequest request) {
        MappingConfig created = service.create(request);
        return SourceConfigResponse.from(created);
    }

    @PutMapping("/{topic}/{sourceEventType}")
    public SourceConfigResponse update(@PathVariable("topic") String topic,
                                       @PathVariable("sourceEventType") String sourceEventType,
                                       @Valid @RequestBody UpdateSourceConfigRequest request) {
        MappingConfig updated = service.update(topic, sourceEventType, request);
        return SourceConfigResponse.from(updated);
    }

    @DeleteMapping("/{topic}/{sourceEventType}")
    public ResponseEntity<Void> delete(@PathVariable("topic") String topic,
                                       @PathVariable("sourceEventType") String sourceEventType) {
        service.delete(topic, sourceEventType);
        return ResponseEntity.noContent().build();
    }
}
