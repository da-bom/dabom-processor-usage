package com.project.domain.usage.service;

import com.dabom.messaging.kafka.event.dto.EventEnvelope;
import com.dabom.messaging.kafka.event.dto.usage.UsagePersistPayload;

public interface UsagePersistService {
    void persist(EventEnvelope<UsagePersistPayload> envelope, String recordKey);
}
