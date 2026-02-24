package com.project.domain.usage.infra.messaging;

import com.project.global.event.dto.usage.UsagePersistPayload;

public interface UsagePersistEventPublisher {
    void publish(UsagePersistPayload payload);
}
