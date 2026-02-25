package com.project.domain.usage.service.helper;

import com.project.domain.usage.infra.messaging.*;
import org.springframework.stereotype.Component;

import com.project.domain.usage.service.dto.UsageUpdateResult;
import com.project.global.event.dto.notification.CustomerBlockedPayload;
import com.project.global.event.dto.notification.ThresholdAlertPayload;
import com.project.global.event.dto.usage.UsagePayload;
import com.project.global.event.dto.usage.UsagePersistPayload;
import com.project.global.event.dto.usage.UsageRealtimePayload;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class UsageEventPublisher {

    private static final String STATUS_WARNING_PREFIX = "WARNING";
    private static final String STATUS_NORMAL_PREFIX = "NORMAL";
    private static final String PERSIST_STATUS_ALLOWED = "ALLOWED";

    // Producers
    private final UsagePersistEventPublisher usagePersistEventPublisher;
    private final UsageRealtimeEventPublisher usageRealtimeEventPublisher;
    private final NotificationEventPublisher notificationEventPublisher;

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

        // DB 저장 이벤트 (Persist)
        usagePersistEventPublisher.publish(
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
                        remaining,
                        ctx.eventTime()));

        // 실시간 사용량 이벤트 (Realtime)
        usageRealtimeEventPublisher.publish(
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

        // 알림 이벤트 (Notification)
        if (status.startsWith(STATUS_WARNING_PREFIX)) {
            int percent = parsePercent(status);
            notificationEventPublisher.publish(
                    new ThresholdAlertPayload(
                            familyId, percent, "가족 데이터가 " + percent + "% 미만입니다!"));

        } else if (!status.startsWith(STATUS_NORMAL_PREFIX)) {
            // reason: TIME_BLOCK, MONTHLY_LIMIT_EXCEEDED, FAMILY_QUOTA_EXCEEDED
            notificationEventPublisher.publish(
                    new CustomerBlockedPayload(familyId, customerId, status, ctx.eventTime()));
        }
    }

    // 임계치 판정
    private int parsePercent(String status) {
        // "WARNING_10" -> 10
        try {
            return Integer.parseInt(status.split("_")[1]);
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            throw new IllegalArgumentException("Invalid warning status format: " + status, e);
        }
    }

    // 이벤트 발행에 필요한 입력을 캡슐화한 내부 컨텍스트 객체
    public record UsageEventContext(
            String eventId, String eventTime, UsagePayload payload, UsageUpdateResult result) {}
}
