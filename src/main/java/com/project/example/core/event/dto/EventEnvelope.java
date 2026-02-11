package com.project.example.core.event.dto;

import java.time.LocalDateTime;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.project.example.core.event.dto.notification.NotificationPayload;
import com.project.example.core.event.dto.policy.PolicyUpdatedPayload;
import com.project.example.core.event.dto.usage.UsagePayload;
import com.project.example.core.event.dto.usage.UsagePersistPayload;
import com.project.example.core.event.dto.usage.UsageRealtimePayload;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true) // 하위 호환성 보장
public record EventEnvelope<T>(
        String eventId,
        String eventType,
        String subType, // Notification인 경우에만 사용 (Nullable)
        LocalDateTime timestamp,
        @JsonTypeInfo(
                        use = JsonTypeInfo.Id.NAME,
                        include = JsonTypeInfo.As.EXTERNAL_PROPERTY,
                        property = "eventType",
                        defaultImpl = Void.class)
                @JsonSubTypes({
                    @JsonSubTypes.Type(value = UsagePayload.class, name = "DATA_USAGE"),
                    @JsonSubTypes.Type(value = PolicyUpdatedPayload.class, name = "POLICY_UPDATED"),
                    @JsonSubTypes.Type(value = UsagePersistPayload.class, name = "USAGE_PERSIST"),
                    @JsonSubTypes.Type(value = NotificationPayload.class, name = "NOTIFICATION"),
                    @JsonSubTypes.Type(value = UsageRealtimePayload.class, name = "USAGE_REALTIME")
                })
                T payload) {
    public static <T> EventEnvelope<T> of(String eventType, T payload) {
        return new EventEnvelope<>(
                UUID.randomUUID().toString(), eventType, null, LocalDateTime.now(), payload);
    }
}
