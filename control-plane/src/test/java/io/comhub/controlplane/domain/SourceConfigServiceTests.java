package io.comhub.controlplane.domain;

import io.comhub.common.config.CanonicalMapping;
import io.comhub.common.config.ConfigCache;
import io.comhub.common.config.ConfigDiscriminator;
import io.comhub.common.config.DiscriminatorSource;
import io.comhub.common.config.MappingConfig;
import io.comhub.common.config.OperationsConfig;
import io.comhub.controlplane.kafka.ConfigTopicPublisher;
import io.comhub.controlplane.web.dto.CreateSourceConfigRequest;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class SourceConfigServiceTests {

    @Test
    void createDerivesSourceEventTypeFromTopicWhenDiscriminatorSourceIsTopic() {
        ConfigTopicPublisher publisher = mock(ConfigTopicPublisher.class);
        SourceConfigService service = new SourceConfigService(new ConfigCache(), publisher);

        service.create(new CreateSourceConfigRequest(
                "orders.failed.v1",
                null,
                true,
                2,
                new ConfigDiscriminator(DiscriminatorSource.TOPIC, null),
                new CanonicalMapping(null, null, null, null, null, List.of()),
                new OperationsConfig(List.of(), List.of(), List.of())));

        ArgumentCaptor<MappingConfig> captor = ArgumentCaptor.forClass(MappingConfig.class);
        verify(publisher).publish(captor.capture());
        assertThat(captor.getValue().sourceEventType()).isEqualTo("orders.failed.v1");
        assertThat(captor.getValue().discriminator().key()).isNull();
    }
}
