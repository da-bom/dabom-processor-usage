package com.project.domain.usage.service;

import java.util.List;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

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

    public void syncUsage(String eventId, String eventTime, UsagePayload payload) {
        long familyId = payload.familyId();

        // 1. Redis Key 생성
        String infoKey = redisKeyGenerator.generateFamilyInfoKey(familyId);
        String remainingKey = redisKeyGenerator.generateFamilyRemainingKey(familyId);

        // 2. Lua Script 실행 (Atomic Update)
        // KEYS: [info, remaining], ARGV: [usageBytes]
        // Result: [totalUsed(Long), remaining(Long), status(String)]
        List<Object> result =
                redisTemplate.execute(
                        usageUpdateScript,
                        List.of(infoKey, remainingKey),
                        String.valueOf(payload.bytesUsed()));

        // 3. 결과 파싱
        long totalUsed = ((Number) result.get(0)).longValue();
        long remaining = ((Number) result.get(1)).longValue();
        String status = (String) result.get(2);

        log.debug(
                "Usage Synced: family={}, used={}, remain={}, status={}",
                familyId,
                totalUsed,
                remaining,
                status);

        // 4. 이벤트 전파
        publishEvents(eventId, eventTime, payload, totalUsed, remaining, status);
    }

    private void publishEvents(
            String eventId,
            String eventTime,
            UsagePayload payload,
            long totalUsed,
            long remaining,
            String status) {

        long familyId = payload.familyId();
        long totalLimit = totalUsed + remaining;
        double usedPercent = totalLimit > 0 ? (double) totalUsed / totalLimit * 100.0 : 0.0;

        // DB 저장 이벤트 발행
        persistProducer.publish(
                new UsagePersistPayload(
                        eventId,
                        familyId,
                        payload.customerId(),
                        payload.bytesUsed(),
                        payload.appId(),
                        "BLOCKED".equals(status) ? "BLOCKED" : "ALLOWED",
                        remaining,
                        eventTime));

        // 사용량 실시간 업데이트 이벤트 발행
        realtimeProducer.publish(
                new UsageRealtimePayload(familyId, totalUsed, totalLimit, remaining, usedPercent));

        // 상태에 따른 알림 발송 이벤트 발행
        if (status.startsWith("WARNING")) {

            int percent = parsePercent(status);

            notificationProducer.publish(
                    new ThresholdAlertPayload(
                            familyId,
                            percent, // 예: 10
                            "가족 데이터가 " + percent + "% 미만입니다!"));
        } else if ("BLOCKED".equals(status)) {
            notificationProducer.publish(
                    new CustomerBlockedPayload(
                            familyId, payload.customerId(), "LIMIT:DATA:MONTHLY", eventTime));
        }
    }

    // 임계치 판정
    private int parsePercent(String status) {
        // "WARNING_10" -> 10
        try {
            return Integer.parseInt(status.split("_")[1]);
        } catch (Exception e) {
            return 10; // 기본값
        }
    }
}
