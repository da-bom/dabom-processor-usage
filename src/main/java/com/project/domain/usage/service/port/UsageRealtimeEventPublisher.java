package com.project.domain.usage.service.port;

import com.project.global.event.dto.usage.UsageRealtimePayload;

public interface UsageRealtimeEventPublisher {
    void publish(UsageRealtimePayload payload);
}
