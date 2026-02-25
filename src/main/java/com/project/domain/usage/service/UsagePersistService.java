package com.project.domain.usage.service;

import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.usage.UsagePersistPayload;

public interface UsagePersistService {
    void persist(EventEnvelope<UsagePersistPayload> envelope, String recordKey);
}
