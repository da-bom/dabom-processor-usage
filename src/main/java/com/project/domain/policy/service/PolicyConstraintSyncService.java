package com.project.domain.policy.service;

import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.policy.PolicyUpdatedPayload;

public interface PolicyConstraintSyncService {
    void sync(EventEnvelope<PolicyUpdatedPayload> envelope, String recordKey);
}
