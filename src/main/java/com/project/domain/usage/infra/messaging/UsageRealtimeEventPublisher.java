package com.project.domain.usage.infra.messaging;

import com.project.global.event.dto.usage.UsageRealtimePayload;

public interface UsageRealtimeEventPublisher {
    void publish(UsageRealtimePayload payload);
}
