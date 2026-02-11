package com.project.example.core.event.dto.notification;

public record ThresholdAlertPayload(
        Long familyId,
        Integer thresholdPercent, // 50, 30, 10
        String message)
        implements NotificationPayload {}
