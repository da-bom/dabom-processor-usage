package com.project.domain.usage.service.helper;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.dabom.messaging.kafka.event.dto.notification.NotificationPayload;
import com.dabom.messaging.kafka.event.dto.notification.NotificationType;
import com.dabom.messaging.kafka.event.dto.usage.UsagePayload;

@Component
public class UsageNotificationPayloadMapper {

    // usage 처리 결과를 notification payload로 변환한다.
    public NotificationPayload toNotificationPayload(
            String eventId,
            LocalDateTime eventDateTime,
            UsagePayload usagePayload,
            String notificationStatus) {
        return switch (notificationStatus) {
            case "WARNING_50" ->
                    buildThresholdAlert(
                            eventId, eventDateTime, usagePayload, notificationStatus, 50);
            case "WARNING_30" ->
                    buildThresholdAlert(
                            eventId, eventDateTime, usagePayload, notificationStatus, 30);
            case "WARNING_10" ->
                    buildThresholdAlert(
                            eventId, eventDateTime, usagePayload, notificationStatus, 10);
            case "MANUAL",
                            "APP_BLOCK",
                            "TIME_BLOCK",
                            "MONTHLY_LIMIT_EXCEEDED",
                            "FAMILY_QUOTA_EXCEEDED" ->
                    buildBlockedAlert(eventId, eventDateTime, usagePayload, notificationStatus);
            default ->
                    throw new IllegalArgumentException(
                            "Unsupported notification status: " + notificationStatus);
        };
    }

    // 경고 알림 payload를 만든다.
    private NotificationPayload buildThresholdAlert(
            String eventId,
            LocalDateTime eventDateTime,
            UsagePayload usagePayload,
            String notificationStatus,
            int threshold) {
        Map<String, Object> data =
                createBaseData(eventId, eventDateTime, usagePayload, notificationStatus);
        data.put("threshold", threshold);

        return new NotificationPayload(
                usagePayload.familyId(),
                usagePayload.customerId(),
                NotificationType.THRESHOLD_ALERT,
                "데이터 사용량 경고",
                "가족 데이터 잔여량이 " + threshold + "% 이하입니다.",
                data);
    }

    // 차단 알림 payload를 만든다.
    private NotificationPayload buildBlockedAlert(
            String eventId,
            LocalDateTime eventDateTime,
            UsagePayload usagePayload,
            String notificationStatus) {
        Map<String, Object> data =
                createBaseData(eventId, eventDateTime, usagePayload, notificationStatus);
        data.put("reason", notificationStatus);

        String message =
                switch (notificationStatus) {
                    case "MANUAL" -> "현재 데이터 사용이 관리자 설정으로 차단되었습니다.";
                    case "APP_BLOCK" -> buildAppBlockMessage(usagePayload.appId());
                    case "TIME_BLOCK" -> "현재 시간에는 데이터 사용이 제한됩니다.";
                    case "MONTHLY_LIMIT_EXCEEDED" -> "개인 월 사용량 한도를 초과했습니다.";
                    case "FAMILY_QUOTA_EXCEEDED" -> "가족 데이터 사용량을 모두 소진했습니다.";
                    default -> "현재 데이터 사용이 차단되었습니다.";
                };

        return new NotificationPayload(
                usagePayload.familyId(),
                usagePayload.customerId(),
                NotificationType.BLOCKED,
                "데이터 사용 차단",
                message,
                data);
    }

    // 앱 차단 알림 문구에 앱 정보를 함께 넣는다.
    private String buildAppBlockMessage(String appId) {
        if (appId == null || appId.isBlank()) {
            return "현재 앱 사용이 차단되어 있습니다.";
        }
        return "현재 앱 사용이 차단되어 있습니다. 대상 앱: " + appId;
    }

    // 공통 추적 정보를 payload data에 담는다.
    private Map<String, Object> createBaseData(
            String eventId,
            LocalDateTime eventDateTime,
            UsagePayload usagePayload,
            String notificationStatus) {
        Map<String, Object> data = new HashMap<>();
        data.put("originEventId", eventId);
        data.put("eventTime", eventDateTime.toString());
        data.put("status", notificationStatus);
        data.put("familyId", usagePayload.familyId());
        data.put("customerId", usagePayload.customerId());
        data.put("appId", usagePayload.appId());
        data.put("bytesUsed", usagePayload.bytesUsed());
        return data;
    }
}
