package com.project.example.core.event.dto.usage;

public record UsagePersistPayload(
        String originEventId,
        Long familyId,
        Long userId,
        Long bytesUsed,
        String appId,
        String processResult, // "ALLOWED", "BLOCKED"
        Long remainingAfter,
        String eventTime) {}
