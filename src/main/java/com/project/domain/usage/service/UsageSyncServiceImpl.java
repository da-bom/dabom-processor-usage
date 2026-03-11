package com.project.domain.usage.service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;

import org.springframework.stereotype.Service;

import com.dabom.messaging.kafka.event.dto.usage.UsagePayload;
import com.project.domain.policy.service.helper.PolicyConstraintWarmupHelper;
import com.project.domain.usage.service.dto.UsageUpdateResult;
import com.project.domain.usage.service.helper.UsageEventPublisher;
import com.project.domain.usage.service.helper.UsageLuaExecutor;
import com.project.domain.usage.service.helper.UsageRedisWarmupHelper;
import com.project.global.common.TimeConstants;
import com.project.global.util.LogSanitizer;
import com.project.global.util.RedisKeyGenerator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class UsageSyncServiceImpl implements UsageSyncService {

    private static final DateTimeFormatter HHMM_FORMATTER = DateTimeFormatter.ofPattern("HHmm");
    private static final String EMPTY_APP_ID = "";

    private final RedisKeyGenerator redisKeyGenerator;

    // Redis Warmup Service
    private final UsageRedisWarmupHelper usageRedisWarmupHelper;
    private final PolicyConstraintWarmupHelper policyConstraintWarmupHelper;

    // Lua Script 실행기
    private final UsageLuaExecutor usageLuaExecutor;

    // 이벤트 발행기
    private final UsageEventPublisher usageEventPublisher;

    // 로그 정리기
    private final LogSanitizer logSanitizer;

    @Override
    public void syncUsage(String eventId, String eventTime, UsagePayload payload) {

        Long familyId = payload.familyId();
        Long customerId = payload.customerId();
        long usageBytes = payload.bytesUsed();
        LocalDateTime resolvedEventDateTime = resolveEventDateTime(eventTime);
        LocalDate eventMonth = resolvedEventDateTime.toLocalDate().withDayOfMonth(1);

        // Redis Key 생성
        String infoKey = redisKeyGenerator.generateFamilyInfoKey(familyId);
        String remainingKey = redisKeyGenerator.generateFamilyRemainingKey(familyId);
        String monthlyKey =
                redisKeyGenerator.generateFamilyCustomerMonthlyUsageKey(
                        familyId, customerId, eventMonth);
        String constraintsKey =
                redisKeyGenerator.generateFamilyCustomerConstraintsKey(familyId, customerId);
        String alertsKey = redisKeyGenerator.generateFamilyAlertsKey(familyId);

        // Warmup
        boolean familyInfoRedisWarmup =
                usageRedisWarmupHelper.ensureFamilyInfoCached(familyId, infoKey);
        boolean familyRemainingRedisWarmup =
                usageRedisWarmupHelper.ensureRemainingBytesCached(familyId, remainingKey);
        boolean customerMonthlyUsageRedisWarmup =
                usageRedisWarmupHelper.ensureCustomerUsageCached(
                        familyId, customerId, monthlyKey, eventMonth);
        policyConstraintWarmupHelper.warmupIfMissing(familyId, customerId);

        if (!familyInfoRedisWarmup
                || !familyRemainingRedisWarmup
                || !customerMonthlyUsageRedisWarmup) {
            log.error("Redis Warmup is Failed. eventId={}", logSanitizer.sanitize(eventId));
            return;
        }

        String currentHhmm = resolvedEventDateTime.format(HHMM_FORMATTER);
        String normalizedAppId = normalizeAppId(payload.appId());

        // Lua Script 실행 + 결과 파싱
        UsageUpdateResult parsed =
                usageLuaExecutor.execute(
                        new UsageLuaExecutor.UsageLuaCommand(
                                infoKey,
                                remainingKey,
                                monthlyKey,
                                constraintsKey,
                                alertsKey,
                                usageBytes,
                                currentHhmm,
                                normalizedAppId),
                        eventId);
        log.debug(
                "Usage Synced: family={}, customer={}, status={}",
                familyId,
                customerId,
                parsed.status());

        // 이벤트 전파
        usageEventPublisher.publish(
                new UsageEventPublisher.UsageEventContext(eventId, eventTime, payload, parsed));
    }

    private LocalDateTime resolveEventDateTime(String eventTime) {
        // producer가 전달한 eventTime이 있으면 우선 사용한다.
        if (eventTime != null && !eventTime.isBlank()) {
            try {
                return LocalDateTime.parse(eventTime);
            } catch (DateTimeParseException ignored) {
                log.debug("Failed to parse eventTime: {}", logSanitizer.sanitize(eventTime));
            }
        }
        // eventTime이 없거나 파싱 실패 시 서버 현재 시각으로 보정한다.
        return LocalDateTime.now(TimeConstants.ASIA_SEOUL);
    }

    private String normalizeAppId(String appId) {
        if (appId == null) {
            return EMPTY_APP_ID;
        }

        String normalized = appId.trim().toLowerCase(Locale.ROOT);
        return normalized.isEmpty() ? EMPTY_APP_ID : normalized;
    }
}
