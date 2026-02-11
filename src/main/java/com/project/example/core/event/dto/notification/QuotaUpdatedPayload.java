package com.project.example.core.event.dto.notification;

public record QuotaUpdatedPayload(
        Long familyId,
        Long userId,
        Long familyRemainingBytes,
        Double familyUsedPercent,
        Long userUsedBytesCurrentMonth)
        implements NotificationPayload {}
