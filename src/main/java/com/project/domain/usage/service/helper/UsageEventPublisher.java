package com.project.domain.usage.service.helper;

import org.springframework.stereotype.Component;

import com.dabom.messaging.kafka.contract.KafkaEventTypes;
import com.dabom.messaging.kafka.contract.KafkaTopics;
import com.dabom.messaging.kafka.event.dto.notification.CustomerBlockedPayload;
import com.dabom.messaging.kafka.event.dto.notification.NotificationEventSupport;
import com.dabom.messaging.kafka.event.dto.notification.ThresholdAlertPayload;
import com.dabom.messaging.kafka.event.dto.usage.UsagePayload;
import com.dabom.messaging.kafka.event.dto.usage.UsagePersistPayload;
import com.dabom.messaging.kafka.event.dto.usage.UsageRealtimePayload;
import com.dabom.messaging.kafka.event.publisher.KafkaEventPublisher;
import com.project.domain.usage.service.dto.UsageUpdateResult;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class UsageEventPublisher {

    private static final String STATUS_APP_BLOCK = "APP_BLOCK";
    private static final String STATUS_WARNING_PREFIX = "WARNING";
    private static final String STATUS_NORMAL_PREFIX = "NORMAL";
    private static final String PERSIST_STATUS_ALLOWED = "ALLOWED";

    private final KafkaEventPublisher kafkaEventPublisher;

    public void publish(UsageEventContext ctx) {

        UsagePayload payload = ctx.payload();
        UsageUpdateResult result = ctx.result();

        long familyId = payload.familyId();
        long customerId = payload.customerId();

        long totalUsed = result.totalUsed();
        long remaining = result.remaining();
        String status = result.status();
        long monthlyUsed = result.monthlyUsed();
        double userRatio = result.userRatio();
        long monthlyLimit = result.monthlyLimit();

        long totalLimit = totalUsed + remaining;
        double usedPercent = totalLimit > 0 ? (double) totalUsed / totalLimit * 100.0 : 0.0;

        if (!STATUS_APP_BLOCK.equals(status)) {
            kafkaEventPublisher.publish(
                    KafkaTopics.USAGE_PERSIST,
                    KafkaEventTypes.USAGE_PERSIST,
                    new UsagePersistPayload(
                            ctx.eventId(),
                            familyId,
                            customerId,
                            payload.bytesUsed(),
                            payload.appId(),
                            status.startsWith(STATUS_WARNING_PREFIX)
                                            || status.equals(STATUS_NORMAL_PREFIX)
                                    ? PERSIST_STATUS_ALLOWED
                                    : status,
                            ctx.eventTime()));

            kafkaEventPublisher.publish(
                    KafkaTopics.USAGE_REALTIME,
                    KafkaEventTypes.USAGE_REALTIME,
                    new UsageRealtimePayload(
                            familyId,
                            customerId,
                            totalUsed,
                            totalLimit,
                            remaining,
                            usedPercent,
                            monthlyUsed,
                            userRatio * 100.0,
                            monthlyLimit));
        }

        if (status.startsWith(STATUS_WARNING_PREFIX)) {
            int percent = parsePercent(status);
            kafkaEventPublisher.publish(
                    KafkaTopics.NOTIFICATION,
                    NotificationEventSupport.toEnvelope(
                            new ThresholdAlertPayload(
                                    familyId,
                                    percent,
                                    String.format("가족 데이터가 %d%% 미만입니다.", percent))));

        } else if (!status.startsWith(STATUS_NORMAL_PREFIX)) {
            kafkaEventPublisher.publish(
                    KafkaTopics.NOTIFICATION,
                    NotificationEventSupport.toEnvelope(
                            new CustomerBlockedPayload(
                                    familyId, customerId, status, ctx.eventTime())));
        }
    }

    private int parsePercent(String status) {
        try {
            return Integer.parseInt(status.split("_")[1]);
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            throw new IllegalArgumentException("Invalid warning status format: " + status, e);
        }
    }

    public record UsageEventContext(
            String eventId, String eventTime, UsagePayload payload, UsageUpdateResult result) {}
}
