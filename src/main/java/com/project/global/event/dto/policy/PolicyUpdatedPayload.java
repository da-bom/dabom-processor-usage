package com.project.global.event.dto.policy;

public record PolicyUpdatedPayload(
        Long familyId,
        Long targetCustomerId,
        String policyKey,
        String oldValue,
        String newValue,
        Long changedBy) {}
