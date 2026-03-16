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
import com.project.domain.policy.service.helper.PolicyConstraintWarmupHelper;
import com.project.domain.usage.service.dto.UsageUpdateResult;
import com.project.domain.usage.service.helper.UsageEventOutboxService;
import com.project.domain.usage.service.helper.UsageFamilyMembershipCacheHelper;
import com.project.domain.usage.service.helper.UsageLuaExecutor;
import com.project.domain.usage.service.helper.UsageNotificationPayloadMapper;
import com.project.domain.usage.service.helper.UsageNotificationPublisher;
import com.project.domain.usage.service.helper.UsageProcessingDecisionMapper;
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

    // usage-events 1건을 Redis, DB, Outbox, notification 흐름으로 처리한다.
    @Override
    public void syncUsage(String eventId, String eventTime, UsagePayload payload) {
        long familyId = payload.familyId();
        long customerId = payload.customerId();

        // 잘못된 family-customer 조합은 초입에서 차단한다.
        validateFamilyMembership(eventId, familyId, customerId);

        // 이후 실패해도 복구 기준점을 잃지 않도록 먼저 PREPARED를 보장한다.
        usageEventOutboxService.ensurePrepared(eventId, familyId, customerId);

        LocalDateTime resolvedEventDateTime = resolveEventDateTime(eventTime);
        LocalDate eventMonth = resolvedEventDateTime.toLocalDate().withDayOfMonth(1);

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
                        familyId, customerId, normalizeAppId(payload.appId()), eventMonth);
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

        ensureWarmupOrThrow(
                eventId, familyId, customerId, eventMonth, infoKey, remainingKey, monthlyKey);

        String currentHhmm = resolvedEventDateTime.format(HHMM_FORMATTER);
        String normalizedAppId = normalizeAppId(payload.appId());

        // Redis Lua에서 중복 여부와 처리 상태를 한 번에 계산한다.
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
                                currentHhmm,
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

        // duplicate라도 PREPARED가 남아 있으면 이전 실패 복구로 보고 재진입한다.
        boolean hasPreparedRow =
                parsed.duplicate() && usageEventOutboxService.hasPreparedRows(eventId);
        if (parsed.duplicate() && !hasPreparedRow) {
            dispatchPendingNotificationIfExists(eventId);
            return;
        }

        // Lua 상태는 중앙 매퍼에서만 해석해 DB 정산과 알림 판단을 일치시킨다.
        UsageProcessingDecisionMapper.UsageProcessingDecision decision =
                usageProcessingDecisionMapper.fromLuaStatus(parsed.status());
        boolean publishNotification = decision.publishNotification() && parsed.shouldNotify();

        if (!parsed.duplicate() || hasPreparedRow) {
            usagePersistService.persistFromUsageEvent(
                    eventId, eventTime, payload, decision.persistProcessResult());
        }

        // Outbox에 최종 notification payload를 저장한 뒤 비동기로 발행한다.
        NotificationPayload notificationPayload =
                publishNotification
                        ? usageNotificationPayloadMapper.toNotificationPayload(
                                eventId,
                                resolvedEventDateTime,
                                payload,
                                decision.notificationStatus())
                        : null;
        usageEventOutboxService
                .stageAfterRedisApplied(eventId, notificationPayload, publishNotification)
                .ifPresent(this::publishAsync);
    }

    // 잘못된 family-customer 조합은 retry하지 않고 즉시 무시한다.
    private void validateFamilyMembership(String eventId, long familyId, long customerId) {
        if (usageFamilyMembershipCacheHelper.isValidFamilyCustomer(familyId, customerId)) {
            return;
        }
        throw new IllegalArgumentException(
                "Invalid family-customer relation. eventId=%s familyId=%d customerId=%d"
                        .formatted(eventId, familyId, customerId));
    }

    // warmup이 끝나지 않으면 Lua를 실행하지 않고 즉시 중단한다.
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

    // 즉시 발행이 가능한 pending notification이 있으면 다시 보낸다.
    private void dispatchPendingNotificationIfExists(String eventId) {
        usageEventOutboxService.findPendingDispatchByEventId(eventId).ifPresent(this::publishAsync);
    }

    // notification을 비동기로 발행하고 성공 시 SENT로 확정한다.
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

    // 이벤트 시각을 파싱하고 실패하면 현재 시각으로 보정한다.
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

    // 앱 차단 비교에 쓰기 쉽게 appId를 정규화한다.
    private String normalizeAppId(String appId) {
        if (appId == null) {
            return EMPTY_APP_ID;
        }
        String normalized = appId.trim().toLowerCase(Locale.ROOT);
        return normalized.isEmpty() ? EMPTY_APP_ID : normalized;
    }
}
