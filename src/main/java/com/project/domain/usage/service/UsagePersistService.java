package com.project.domain.usage.service;

import com.dabom.messaging.kafka.event.dto.usage.UsagePayload;

public interface UsagePersistService {
    void persistFromUsageEvent(
            String eventId, String eventTime, UsagePayload usagePayload, String processResult);
}
