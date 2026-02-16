package com.project.domain.usage.service;

import java.util.List;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.project.domain.notification.infra.messaging.NotificationKafkaProducer;
import com.project.domain.usage.infra.messaging.UsagePersistKafkaProducer;
import com.project.domain.usage.infra.messaging.UsageRealtimeKafkaProducer;
import com.project.global.event.dto.notification.CustomerBlockedPayload;
import com.project.global.event.dto.notification.ThresholdAlertPayload;
import com.project.global.event.dto.usage.UsagePayload;
import com.project.global.event.dto.usage.UsagePersistPayload;
import com.project.global.event.dto.usage.UsageRealtimePayload;
import com.project.global.util.RedisKeyGenerator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class UsageSyncService {

    private final StringRedisTemplate redisTemplate;
    private final RedisKeyGenerator redisKeyGenerator;

    // Producers
    private final UsagePersistKafkaProducer persistProducer;
    private final UsageRealtimeKafkaProducer realtimeProducer;
    private final NotificationKafkaProducer notificationProducer;

    // Lua Script
    private final RedisScript<List<Object>> usageUpdateScript;

    @Transactional
    public void syncUsage(String eventId, String eventTime, UsagePayload payload) {

        Long familyId = payload.familyId();
        Long customerId = payload.customerId();
        long usageBytes = payload.bytesUsed();

        // Redis Key 생성
        String infoKey = redisKeyGenerator.generateFamilyInfoKey(familyId);
        String remainingKey = redisKeyGenerator.generateFamilyRemainingKey(familyId);
        String monthlyKey =
                redisKeyGenerator.generateFamilyCustomerMonthlyUsageKey(familyId, customerId);
        String constraintsKey =
                redisKeyGenerator.generateFamilyCustomerConstraintsKey(familyId, customerId);
        String alertsKey = redisKeyGenerator.generateFamilyAlertsKey(familyId);

        // Lua Script 실행
        List<Object> result =
                redisTemplate.execute(
                        usageUpdateScript,
                        List.of(infoKey, remainingKey, monthlyKey, constraintsKey, alertsKey),
                        String.valueOf(usageBytes));
        if (result == null || result.isEmpty()) {
            log.error("Usage update script returned null. eventId={}", eventId);
            return;
        }

        // 결과 파싱
        long totalUsed = ((Number) result.get(0)).longValue();
        long remaining = ((Number) result.get(1)).longValue();
        String status = (String) result.get(2);
        long monthlyUsed = ((Number) result.get(3)).longValue();

        Object userRatioObj = result.get(4);
        double userRatio =
                (userRatioObj instanceof Number)
                        ? ((Number) userRatioObj).doubleValue()
                        : Double.parseDouble(userRatioObj.toString());

        long monthlyLimit = ((Number) result.get(5)).longValue();
        log.debug("Usage Synced: family={}, customer={}, status={}", familyId, customerId, status);

        // 이벤트 전파
        publishEvents(
                eventId,
                eventTime,
                payload,
                totalUsed,
                remaining,
                status,
                monthlyUsed,
                userRatio,
                monthlyLimit);
    }

    private void publishEvents(
            String eventId,
            String eventTime,
            UsagePayload payload,
            long totalUsed,
            long remaining,
            String status,
            long monthlyUsed,
            double userRatio,
            long monthlyLimit) {

        long familyId = payload.familyId();
        long customerId = payload.customerId();
        long totalLimit = totalUsed + remaining;
        double usedPercent = totalLimit > 0 ? (double) totalUsed / totalLimit * 100.0 : 0.0;

        // DB 저장 이벤트 (Persist)
        persistProducer.publish(
                new UsagePersistPayload(
                        eventId,
                        familyId,
                        customerId,
                        payload.bytesUsed(),
                        payload.appId(),
                        status.startsWith("BLOCKED") ? "BLOCKED" : "ALLOWED",
                        remaining,
                        eventTime));

        // 실시간 사용량 이벤트 (Realtime)
        realtimeProducer.publish(
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
        if (status.startsWith("WARNING")) {
            int percent = parsePercent(status);
            notificationProducer.publish(
                    new ThresholdAlertPayload(
                            familyId, percent, "가족 데이터가 " + percent + "% 미만입니다!"));

        } else if (status.startsWith("BLOCKED")) {
            // reason: BLOCKED_ACCESS, BLOCKED_LIMIT_MONTHLY, BLOCKED_FAMILY_QUOTA
            notificationProducer.publish(
                    new CustomerBlockedPayload(familyId, customerId, status, eventTime));
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
}
