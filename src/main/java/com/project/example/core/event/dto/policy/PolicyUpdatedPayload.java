package com.project.example.core.event.dto.policy;

public record PolicyUpdatedPayload(
        Long familyId,
        Long targetUserId,
        String policyKey, // 예: "LIMIT:DATA:DAILY", "BLOCK:APP:com.youtube"
        String oldValue,
        String newValue,
        Long changedBy) {}
