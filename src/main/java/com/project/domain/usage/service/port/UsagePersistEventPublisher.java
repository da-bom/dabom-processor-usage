package com.project.domain.usage.service.port;

import com.project.global.event.dto.usage.UsagePersistPayload;

public interface UsagePersistEventPublisher {
    void publish(UsagePersistPayload payload);
}
