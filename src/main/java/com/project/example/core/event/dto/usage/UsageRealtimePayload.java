package com.project.example.core.event.dto.usage;

public record UsageRealtimePayload(
        Long familyId,
        Long totalUsedBytes,
        Long totalLimitBytes,
        Long remainingBytes,
        Double usedPercent) {}
