package com.project.domain.usage.service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.dabom.messaging.kafka.contract.KafkaConsumerGroups;
import com.dabom.messaging.kafka.contract.KafkaEventTypes;
import com.dabom.messaging.kafka.contract.KafkaTopics;
import com.dabom.messaging.kafka.error.KafkaMessageProcessingException;
import com.dabom.messaging.kafka.event.dto.notification.NotificationPayload;
import com.dabom.messaging.kafka.event.dto.usage.UsagePayload;
import com.dabom.messaging.kafka.metrics.KafkaMetrics;
import com.project.common.config.TimeConfig;
import com.project.common.util.LogSanitizer;
import com.project.common.util.RedisKeyGenerator;
import com.project.domain.policy.helper.PolicyConstraintWarmupHelper;
import com.project.domain.usage.service.dto.UsageUpdateResult;
import com.project.domain.usage.service.helper.UsageEventOutboxService;
import com.project.domain.usage.service.helper.UsageFamilyMembershipCacheHelper;
import com.project.domain.usage.service.helper.UsageLuaExecutor;
import com.project.domain.usage.service.helper.UsageNotificationPayloadMapper;
import com.project.domain.usage.service.helper.UsageNotificationPublisher;
import com.project.domain.usage.service.helper.UsageProcessingDecisionMapper;
import com.project.domain.usage.service.helper.UsageRedisWarmupHelper;

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
    private final UsagePersistService usagePersistService;
    private final UsageEventOutboxService usageEventOutboxService;
    private final UsageProcessingDecisionMapper usageProcessingDecisionMapper;
    private final UsageNotificationPayloadMapper usageNotificationPayloadMapper;
    private final UsageNotificationPublisher usageNotificationPublisher;
    private final UsageFamilyMembershipCacheHelper usageFamilyMembershipCacheHelper;
    private final LogSanitizer logSanitizer;
    private final KafkaMetrics kafkaMetrics;

    @Value("${app.kafka.dedup.usage-ttl-seconds}")
    private long dedupTtlSeconds;

    // usage-events 1건을 검증하고 Redis/Lua/DB 정산/알림 발행까지 처리한다.
    @Override
    public void syncUsage(String eventId, String eventTime, UsagePayload payload) {
        long familyId = payload.familyId();
        long customerId = payload.customerId();

        // 잘못된 family-customer 조합은 초입에서 바로 차단한다.
        validateFamilyMembership(eventId, familyId, customerId);

        LocalDateTime resolvedEventDateTime = resolveEventDateTime(eventTime);
        LocalDate eventMonth = resolvedEventDateTime.toLocalDate().withDayOfMonth(1);

        String normalizedAppId = normalizeAppId(payload.appId());
        String infoKey = redisKeyGenerator.generateFamilyInfoKey(familyId, eventMonth);
        String remainingKey = redisKeyGenerator.generateFamilyRemainingKey(familyId, eventMonth);
        String monthlyKey =
                redisKeyGenerator.generateFamilyCustomerMonthlyUsageKey(
                        familyId, customerId, eventMonth);
        String constraintsKey =
                redisKeyGenerator.generateFamilyCustomerConstraintsKey(familyId, customerId);
        String alert50Key =
                redisKeyGenerator.generateFamilyCustomerThresholdAlertKey(
                        familyId, customerId, 50, eventMonth);
        String alert30Key =
                redisKeyGenerator.generateFamilyCustomerThresholdAlertKey(
                        familyId, customerId, 30, eventMonth);
        String alert10Key =
                redisKeyGenerator.generateFamilyCustomerThresholdAlertKey(
                        familyId, customerId, 10, eventMonth);
        String manualAlertKey =
                redisKeyGenerator.generateFamilyCustomerBlockAlertKey(
                        familyId, customerId, "MANUAL", eventMonth);
        String appBlockAlertKey =
                redisKeyGenerator.generateFamilyCustomerAppBlockAlertKey(
                        familyId, customerId, normalizedAppId, eventMonth);
        String timeBlockAlertKey =
                redisKeyGenerator.generateFamilyCustomerBlockAlertKey(
                        familyId, customerId, "TIME_BLOCK", eventMonth);
        String monthlyLimitAlertKey =
                redisKeyGenerator.generateFamilyCustomerBlockAlertKey(
                        familyId, customerId, "MONTHLY_LIMIT_EXCEEDED", eventMonth);
        String familyQuotaAlertKey =
                redisKeyGenerator.generateFamilyCustomerBlockAlertKey(
                        familyId, customerId, "FAMILY_QUOTA_EXCEEDED", eventMonth);
        String dedupKey = redisKeyGenerator.generateUsageEventDedupKey(eventId);

        // Redis 상태가 준비되지 않으면 Lua 판단을 태우지 않는다.
        ensureWarmupOrThrow(
                eventId, familyId, customerId, eventMonth, infoKey, remainingKey, monthlyKey);

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
                                manualAlertKey,
                                appBlockAlertKey,
                                timeBlockAlertKey,
                                monthlyLimitAlertKey,
                                familyQuotaAlertKey,
                                dedupKey,
                                payload.bytesUsed(),
                                resolvedEventDateTime.format(HHMM_FORMATTER),
                                normalizedAppId,
                                dedupTtlSeconds),
                        eventId);

        log.debug(
                "Usage synced: family={}, customer={}, status={}, notify={}, duplicate={}",
                familyId,
                customerId,
                parsed.status(),
                parsed.shouldNotify(),
                parsed.duplicate());

        if (parsed.duplicate()) {
            kafkaMetrics.incrementDedupHit(
                    KafkaTopics.USAGE_EVENTS,
                    KafkaConsumerGroups.DABOM_PROCESSOR_USAGE_MAIN,
                    KafkaEventTypes.DATA_USAGE);
            log.info(
                    "Duplicate usage event. eventId={}, familyId={}, customerId={}",
                    logSanitizer.sanitize(eventId),
                    familyId,
                    customerId);
        }

        // Lua 상태는 중앙 매퍼에서만 해석한다.
        UsageProcessingDecisionMapper.UsageProcessingDecision decision =
                usageProcessingDecisionMapper.fromLuaStatus(parsed.status());

        // DB 정산은 usage_record unique와 quota 갱신 규칙으로 멱등하게 재진입한다.
        usagePersistService.persistFromUsageEvent(
                eventId, eventTime, payload, decision.persistProcessResult());

        boolean publishNotification = decision.publishNotification() && parsed.shouldNotify();
        if (!publishNotification) {
            dispatchPendingNotificationIfExists(eventId);
            return;
        }

        NotificationPayload notificationPayload =
                usageNotificationPayloadMapper.toNotificationPayload(
                        eventId, resolvedEventDateTime, payload, decision.notificationStatus());

        usageEventOutboxService
                .stageAfterRedisApplied(eventId, notificationPayload, true)
                .ifPresent(this::publishAsync);
    }

    // family-customer 관계가 틀리면 invalid payload로 간주하고 중단한다.
    private void validateFamilyMembership(String eventId, long familyId, long customerId) {
        if (usageFamilyMembershipCacheHelper.isValidFamilyCustomer(familyId, customerId)) {
            return;
        }
        throw new IllegalArgumentException(
                "Invalid family-customer relation. eventId=%s familyId=%d customerId=%d"
                        .formatted(eventId, familyId, customerId));
    }

    // warmup 실패는 일시 장애로 보고 retryable 예외로 전파한다.
    private void ensureWarmupOrThrow(
            String eventId,
            long familyId,
            long customerId,
            LocalDate eventMonth,
            String infoKey,
            String remainingKey,
            String monthlyKey) {
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
            log.error("Redis warmup failed. eventId={}", logSanitizer.sanitize(eventId));
            throw new KafkaMessageProcessingException(
                    "Redis warmup failed. eventId=%s familyId=%d customerId=%d"
                            .formatted(eventId, familyId, customerId),
                    new IllegalStateException("Redis warmup failed"));
        }
    }

    // 이미 만들어진 pending notification이 있으면 다시 즉시 발행을 시도한다.
    private void dispatchPendingNotificationIfExists(String eventId) {
        usageEventOutboxService.findPendingDispatchByEventId(eventId).ifPresent(this::publishAsync);
    }

    // notification은 비동기로 발행하고 성공 시에만 SENT로 마감한다.
    private void publishAsync(UsageEventOutboxService.PendingNotificationDispatch pending) {
        usageNotificationPublisher
                .publishAsync(pending.payload())
                .whenComplete(
                        (SendResult<String, String> ignored, Throwable throwable) -> {
                            if (throwable == null) {
                                usageEventOutboxService.markSent(pending.outboxId());
                                return;
                            }

                            log.warn(
                                    "Notification publish deferred to batch retry. outboxId={},"
                                            + " reason={}",
                                    pending.outboxId(),
                                    throwable.getMessage());
                        });
    }

    // eventTime을 파싱하고 실패하면 현재 시각으로 보정한다.
    private LocalDateTime resolveEventDateTime(String eventTime) {
        if (eventTime != null && !eventTime.isBlank()) {
            try {
                return LocalDateTime.parse(eventTime);
            } catch (DateTimeParseException ignored) {
                log.debug("Failed to parse eventTime: {}", logSanitizer.sanitize(eventTime));
            }
        }
        return LocalDateTime.now(TimeConfig.ASIA_SEOUL);
    }

    // 앱 차단 키 비교에 사용하도록 appId를 정규화한다.
    private String normalizeAppId(String appId) {
        if (appId == null) {
            return EMPTY_APP_ID;
        }
        String normalized = appId.trim().toLowerCase(Locale.ROOT);
        return normalized.isEmpty() ? EMPTY_APP_ID : normalized;
    }
}
