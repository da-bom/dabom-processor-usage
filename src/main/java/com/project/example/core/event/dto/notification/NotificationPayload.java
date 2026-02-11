package com.project.example.core.event.dto.notification;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXTERNAL_PROPERTY,
        property = "subType",
        defaultImpl = Void.class)
@JsonSubTypes({
    @JsonSubTypes.Type(value = QuotaUpdatedPayload.class, name = "QUOTA_UPDATED"),
    @JsonSubTypes.Type(value = UserBlockedPayload.class, name = "USER_BLOCKED"),
    @JsonSubTypes.Type(value = ThresholdAlertPayload.class, name = "THRESHOLD_ALERT")
})
public sealed interface NotificationPayload
        permits QuotaUpdatedPayload, UserBlockedPayload, ThresholdAlertPayload {}
