package com.project.domain.policy.service;

import com.dabom.messaging.kafka.event.dto.EventEnvelope;
import com.dabom.messaging.kafka.event.dto.policy.PolicyUpdatedPayload;

public interface PolicyConstraintSyncService {
    void sync(EventEnvelope<PolicyUpdatedPayload> envelope, String recordKey);
}
