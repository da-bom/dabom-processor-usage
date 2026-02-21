package com.project.domain.usage.service;

import java.util.List;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import com.project.domain.notification.infra.messaging.NotificationKafkaProducer;
import com.project.domain.policy.service.PolicyConstraintWarmupService;
import com.project.domain.usage.infra.messaging.UsagePersistKafkaProducer;
import com.project.domain.usage.infra.messaging.UsageRealtimeKafkaProducer;
import com.project.domain.usage.service.dto.UsageUpdateResult;
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

    private static final String STATUS_BLOCKED_PREFIX = "BLOCKED";
    private static final String STATUS_WARNING_PREFIX = "WARNING";

    private static final String PERSIST_STATUS_BLOCKED = "BLOCKED";
    private static final String PERSIST_STATUS_ALLOWED = "ALLOWED";

    private final StringRedisTemplate redisTemplate;
    private final RedisKeyGenerator redisKeyGenerator;

    // Redis Warmup Service
    private final UsageRedisWarmupService usageRedisWarmupService;
    private final PolicyConstraintWarmupService policyConstraintWarmupService;

    // Producers
    private final UsagePersistKafkaProducer persistProducer;
    private final UsageRealtimeKafkaProducer realtimeProducer;
    private final NotificationKafkaProducer notificationProducer;

    // Lua Script
    private final RedisScript<List<Object>> usageUpdateScript;

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

        // Warmup
        boolean familyInfoRedisWarmup =
                usageRedisWarmupService.ensureFamilyInfoCached(familyId, infoKey);
        boolean familyRemainingRedisWarmup =
                usageRedisWarmupService.ensureRemainingBytesCached(familyId, remainingKey);
        boolean customerMonthlyUsageRedisWarmup =
                usageRedisWarmupService.ensureCustomerUsageCached(familyId, customerId, monthlyKey);
        policyConstraintWarmupService.warmupIfMissing(familyId, customerId);

        if (!(familyInfoRedisWarmup
                && familyRemainingRedisWarmup
                && customerMonthlyUsageRedisWarmup)) {
            log.error("Redis Warmup is Failed. eventId={}", eventId);
            return;
        }

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

        UsageUpdateResult parsed = parseScriptResult(result, eventId);
        log.debug(
                "Usage Synced: family={}, customer={}, status={}",
                familyId,
                customerId,
                parsed.status());

        UsageSyncContext ctx = new UsageSyncContext(eventId, eventTime, payload, parsed);

        // 이벤트 전파
        publishEvents(ctx);
    }

    private void publishEvents(UsageSyncContext ctx) {

        UsagePayload payload = ctx.payload();

        long familyId = payload.familyId();
        long customerId = payload.customerId();

        long totalUsed = ctx.result().totalUsed();
        long remaining = ctx.result().remaining();
        String status = ctx.result().status();
        long monthlyUsed = ctx.result().monthlyUsed();
        double userRatio = ctx.result().userRatio();
        long monthlyLimit = ctx.result().monthlyLimit();

        long totalLimit = totalUsed + remaining;
        double usedPercent = totalLimit > 0 ? (double) totalUsed / totalLimit * 100.0 : 0.0;

        // DB 저장 이벤트 (Persist)
        persistProducer.publish(
                new UsagePersistPayload(
                        ctx.eventId(),
                        familyId,
                        customerId,
                        payload.bytesUsed(),
                        payload.appId(),
                        status.startsWith(STATUS_BLOCKED_PREFIX)
                                ? PERSIST_STATUS_BLOCKED
                                : PERSIST_STATUS_ALLOWED,
                        remaining,
                        ctx.eventTime()));

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
        if (status.startsWith(STATUS_WARNING_PREFIX)) {
            int percent = parsePercent(status);
            notificationProducer.publish(
                    new ThresholdAlertPayload(
                            familyId, percent, "가족 데이터가 " + percent + "% 미만입니다!"));

        } else if (status.startsWith(STATUS_BLOCKED_PREFIX)) {
            // reason: BLOCKED_ACCESS, BLOCKED_LIMIT_MONTHLY, BLOCKED_FAMILY_QUOTA
            notificationProducer.publish(
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

    // lua script 결과 파싱
    private UsageUpdateResult parseScriptResult(List<Object> result, String eventId) {
        if (result == null || result.size() < 6) {
            log.error(
                    "Usage update script returned invalid result. eventId={}, result={}",
                    eventId,
                    result);
            throw new IllegalStateException("Invalid Lua script result");
        }

        long totalUsed = ((Number) result.get(0)).longValue();
        long remaining = ((Number) result.get(1)).longValue();
        String status = (String) result.get(2);
        long monthlyUsed = ((Number) result.get(3)).longValue();

        Object userRatioObj = result.get(4);
        double userRatio =
                (userRatioObj instanceof Number number)
                        ? number.doubleValue()
                        : Double.parseDouble(userRatioObj.toString());

        long monthlyLimit = ((Number) result.get(5)).longValue();

        return new UsageUpdateResult(
                totalUsed, remaining, status, monthlyUsed, userRatio, monthlyLimit);
    }

    private record UsageSyncContext(
            String eventId, String eventTime, UsagePayload payload, UsageUpdateResult result) {}
}
