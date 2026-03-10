package com.project.domain.usage.service;

import com.dabom.messaging.kafka.event.dto.usage.UsagePayload;

public interface UsageSyncService {
    void syncUsage(String eventId, String eventTime, UsagePayload payload);
}
