package com.project.example.core.event.dto.notification;

public record UserBlockedPayload(
        Long familyId,
        Long userId,
        String blockReason, // 예: "LIMIT:DATA:DAILY"
        String blockedAt)
        implements NotificationPayload {}
