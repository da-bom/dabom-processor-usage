package com.project.domain.usage.service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.dabom.messaging.kafka.contract.KafkaConsumerGroups;
import com.dabom.messaging.kafka.contract.KafkaEventTypes;
import com.dabom.messaging.kafka.contract.KafkaTopics;
import com.dabom.messaging.kafka.event.dto.usage.UsagePayload;
import com.dabom.messaging.kafka.metrics.KafkaMetrics;
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
    private final UsageRedisWarmupHelper usageRedisWarmupHelper;
    private final PolicyConstraintWarmupHelper policyConstraintWarmupHelper;
    private final UsageLuaExecutor usageLuaExecutor;
    private final UsageEventPublisher usageEventPublisher;
    private final LogSanitizer logSanitizer;
    private final KafkaMetrics kafkaMetrics;

    @Value("${app.kafka.dedup.usage-ttl-seconds}")
    private long dedupTtlSeconds;

    @Override
    public void syncUsage(String eventId, String eventTime, UsagePayload payload) {

        Long familyId = payload.familyId();
        Long customerId = payload.customerId();
        long usageBytes = payload.bytesUsed();

        // 1) eventTime 해석 + 월 키 기준 계산
        LocalDateTime resolvedEventDateTime = resolveEventDateTime(eventTime);
        LocalDate eventMonth = resolvedEventDateTime.toLocalDate().withDayOfMonth(1);

        // 2) Lua 실행에 필요한 Redis 키 생성
        String infoKey = redisKeyGenerator.generateFamilyInfoKey(familyId, eventMonth);
        String remainingKey = redisKeyGenerator.generateFamilyRemainingKey(familyId, eventMonth);
        String monthlyKey =
                redisKeyGenerator.generateFamilyCustomerMonthlyUsageKey(
                        familyId, customerId, eventMonth);
        String constraintsKey =
                redisKeyGenerator.generateFamilyCustomerConstraintsKey(familyId, customerId);
        String alert50Key = redisKeyGenerator.generateFamilyAlertKey(familyId, 50, eventMonth);
        String alert30Key = redisKeyGenerator.generateFamilyAlertKey(familyId, 30, eventMonth);
        String alert10Key = redisKeyGenerator.generateFamilyAlertKey(familyId, 10, eventMonth);
        String dedupKey = redisKeyGenerator.generateUsageEventDedupKey(eventId);

        // 3) Redis warmup 보장
        boolean familyInfoRedisWarmup =
                usageRedisWarmupHelper.ensureFamilyInfoCached(familyId, eventMonth, infoKey);
        boolean familyRemainingRedisWarmup =
                usageRedisWarmupHelper.ensureRemainingBytesCached(
                        familyId, eventMonth, remainingKey);
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

        // 4) Lua로 정책 판정 + 사용량 반영 + dedup 검사 수행
        UsageUpdateResult parsed =
                usageLuaExecutor.execute(
                        new UsageLuaExecutor.UsageLuaCommand(
                                infoKey,
                                remainingKey,
                                monthlyKey,
                                constraintsKey,
                                alert50Key,
                                alert30Key,
                                alert10Key,
                                dedupKey,
                                usageBytes,
                                currentHhmm,
                                normalizedAppId,
                                dedupTtlSeconds),
                        eventId);
        log.debug(
                "Usage Synced: family={}, customer={}, status={}",
                familyId,
                customerId,
                parsed.status());

        // 5) duplicate면 후속 publish 없이 종료
        if (parsed.duplicate()) {
            kafkaMetrics.incrementDedupHit(
                    KafkaTopics.USAGE_EVENTS,
                    KafkaConsumerGroups.DABOM_PROCESSOR_USAGE_MAIN,
                    KafkaEventTypes.DATA_USAGE);
            log.info(
                    "Skip duplicated usage event. eventId={}, familyId={}, customerId={}",
                    logSanitizer.sanitize(eventId),
                    familyId,
                    customerId);
            return;
        }

        // 6) downstream 이벤트 발행
        usageEventPublisher.publish(
                new UsageEventPublisher.UsageEventContext(eventId, eventTime, payload, parsed));
    }

    private LocalDateTime resolveEventDateTime(String eventTime) {
        if (eventTime != null && !eventTime.isBlank()) {
            try {
                return LocalDateTime.parse(eventTime);
            } catch (DateTimeParseException ignored) {
                log.debug("Failed to parse eventTime: {}", logSanitizer.sanitize(eventTime));
            }
        }
        return LocalDateTime.now(TimeConstants.ASIA_SEOUL);
    }

    private String normalizeAppId(String appId) {
        if (appId == null) {
            return EMPTY_APP_ID;
        }

        // app 차단 정책 키와 비교할 수 있게 정규화
        String normalized = appId.trim().toLowerCase(Locale.ROOT);
        return normalized.isEmpty() ? EMPTY_APP_ID : normalized;
    }
}
