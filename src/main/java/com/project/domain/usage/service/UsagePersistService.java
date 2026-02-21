package com.project.domain.usage.service;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.project.domain.customer.entity.CustomerQuota;
import com.project.domain.customer.repository.CustomerQuotaRepository;
import com.project.domain.usage.entity.UsageRecord;
import com.project.domain.usage.repository.UsageRecordRepository;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.usage.UsagePersistPayload;
import com.project.global.util.LogSanitizer;
import com.project.global.util.RedisKeyGenerator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class UsagePersistService {
    private static final String ASIA_SEOUL_TIME_ZONE = "Asia/Seoul";
    private static final String DEDUP_FLAG = "1";
    private static final ZoneId KST = ZoneId.of(ASIA_SEOUL_TIME_ZONE);
    private static final String USAGE_PERSIST_LOG_SUFFIX =
            " familyId={}, customerId={}, bytesUsed={}, currentMonth={}";
    private static final long ALLOWED_PAST_MONTHS = 1;
    private static final long ALLOWED_FUTURE_MONTHS = 0;
    private static final long SAFE_DEFAULT_MONTHLY_LIMIT_BYTES = 0L;

    private final RedisTemplate<String, String> familyStringRedisTemplate;
    private final RedisKeyGenerator redisKeyGenerator;
    private final LogSanitizer logSanitizer;
    private final UsagePersistEventValidator usagePersistEventValidator;
    private final CustomerQuotaRepository customerQuotaRepository;
    private final UsageRecordRepository usageRecordRepository;

    @Value("${app.kafka.dedup.usage-persist-ttl-seconds}")
    private long usagePersistDedupTtlSeconds;

    @Transactional
    public void persist(EventEnvelope<UsagePersistPayload> envelope, String recordKey) {
        UsagePersistPayload payload = envelope.payload();
        String eventId = envelope.eventId();

        // 1) payload 검증
        if (!isValidPayload(payload, eventId, recordKey)) {
            return;
        }

        if (payload == null) {
            return;
        }

        String originEventId = payload.originEventId();

        // 2) 중복 이벤트 차단
        if (isDuplicated(originEventId)) {
            return;
        }

        // 3) 기준 월 계산
        LocalDate currentMonth = resolveCurrentMonth(payload.eventTime());

        // 4) usage_record 저장(event_id 유니크로 멱등 보장)
        if (!persistUsageRecord(payload, eventId, originEventId)) {
            return;
        }

        // 5) DB update 우선, 없으면 insert(+경합 시 update 재시도)
        persistQuota(payload, currentMonth, eventId, originEventId);
    }

    private boolean isValidPayload(UsagePersistPayload payload, String eventId, String recordKey) {
        return usagePersistEventValidator.isValidPayload(payload, eventId, recordKey);
    }

    private void persistQuota(
            UsagePersistPayload payload,
            LocalDate currentMonth,
            String eventId,
            String originEventId) {
        // 이미 row가 있으면 원자적 증가(update)로 끝내고, 없을 때만 insert 경로로 진입
        if (tryUpdateExistingQuota(payload, currentMonth, eventId, originEventId)) {
            return;
        }

        createQuotaRow(payload, currentMonth, eventId, originEventId);
    }

    private boolean tryUpdateExistingQuota(
            UsagePersistPayload payload,
            LocalDate currentMonth,
            String eventId,
            String originEventId) {
        // 월별 quota row가 존재하는 일반 케이스: update 1회로 처리
        int updatedRows =
                customerQuotaRepository.incrementMonthlyUsedBytes(
                        payload.familyId(),
                        payload.customerId(),
                        currentMonth,
                        payload.bytesUsed());
        if (updatedRows <= 0) {
            return false;
        }

        log.info(
                "Persisted usage to existing quota row. eventId={}, originEventId={},"
                        + USAGE_PERSIST_LOG_SUFFIX,
                logSanitizer.sanitize(eventId),
                logSanitizer.sanitize(originEventId),
                payload.familyId(),
                payload.customerId(),
                payload.bytesUsed(),
                currentMonth);
        return true;
    }

    private void createQuotaRow(
            UsagePersistPayload payload,
            LocalDate currentMonth,
            String eventId,
            String originEventId) {
        // 신규 월 row 생성 시, 직전 월의 limit 값을 기본값으로 이어받는다
        long monthlyLimitBytes = resolveMonthlyLimitBytes(payload.familyId(), payload.customerId());
        CustomerQuota customerQuota = buildCustomerQuota(payload, currentMonth, monthlyLimitBytes);
        try {
            customerQuotaRepository.saveAndFlush(customerQuota);
        } catch (DataIntegrityViolationException e) {
            // insert 경합 시 update 재시도
            if (retryUpdateAfterInsertRace(payload, currentMonth, eventId, originEventId)) {
                return;
            }
            throw e;
        }

        log.info(
                "Persisted usage by creating quota row. eventId={}, originEventId={},"
                        + USAGE_PERSIST_LOG_SUFFIX,
                logSanitizer.sanitize(eventId),
                logSanitizer.sanitize(originEventId),
                payload.familyId(),
                payload.customerId(),
                payload.bytesUsed(),
                currentMonth);
    }

    private boolean retryUpdateAfterInsertRace(
            UsagePersistPayload payload,
            LocalDate currentMonth,
            String eventId,
            String originEventId) {
        // 동시성으로 다른 트랜잭션이 먼저 insert한 경우를 update 재시도로 복구
        int retriedRows =
                customerQuotaRepository.incrementMonthlyUsedBytes(
                        payload.familyId(),
                        payload.customerId(),
                        currentMonth,
                        payload.bytesUsed());
        if (retriedRows <= 0) {
            return false;
        }

        log.info(
                "Recovered from concurrent insert race. eventId={}, originEventId={},"
                        + USAGE_PERSIST_LOG_SUFFIX,
                logSanitizer.sanitize(eventId),
                logSanitizer.sanitize(originEventId),
                payload.familyId(),
                payload.customerId(),
                payload.bytesUsed(),
                currentMonth);
        return true;
    }

    private CustomerQuota buildCustomerQuota(
            UsagePersistPayload payload, LocalDate currentMonth, long monthlyLimitBytes) {
        return CustomerQuota.builder()
                .familyId(payload.familyId())
                .customerId(payload.customerId())
                .monthlyUsedBytes(payload.bytesUsed())
                .currentMonth(currentMonth)
                .monthlyLimitBytes(monthlyLimitBytes)
                .isBlocked(false)
                .blockReason(null)
                .build();
    }

    private boolean isDuplicated(String originEventId) {
        String dedupKey = redisKeyGenerator.generateUsagePersistEventDedupKey(originEventId);
        try {
            // Redis setIfAbsent + TTL로 짧은 윈도우 중복 이벤트를 제거한다
            Boolean firstSeen =
                    familyStringRedisTemplate
                            .opsForValue()
                            .setIfAbsent(
                                    dedupKey,
                                    DEDUP_FLAG,
                                    Duration.ofSeconds(usagePersistDedupTtlSeconds));
            if (!Boolean.TRUE.equals(firstSeen)) {
                log.info(
                        "Skip duplicated usage-persist event. originEventId={}",
                        logSanitizer.sanitize(originEventId));
                return true;
            }
            return false;
        } catch (DataAccessException e) {
            // 가용성 우선: Redis 장애 시에도 DB 적재는 계속 진행한다
            log.warn(
                    "Redis dedup failed. Continue DB persist for availability. originEventId={}",
                    logSanitizer.sanitize(originEventId),
                    e);
            return false;
        }
    }

    private LocalDate resolveCurrentMonth(String eventTime) {
        // 기본값은 현재 KST 월의 1일(yyyy-MM-01)
        LocalDate currentMonth = LocalDate.now(KST).withDayOfMonth(1);
        if (eventTime == null || eventTime.isBlank()) {
            return currentMonth;
        }
        try {
            // eventTime이 과도한 과거/미래 월이면 현재 월 fallback (past=1, future=0)
            LocalDate parsedMonth =
                    OffsetDateTime.parse(eventTime)
                            .atZoneSameInstant(KST)
                            .toLocalDate()
                            .withDayOfMonth(1);
            if (isOutsideAllowedMonthWindow(parsedMonth, currentMonth)) {
                log.warn(
                        "Suspicious eventTime month. Fallback to current month. eventTime={},"
                                + " parsedMonth={}, currentMonth={}, allowedPastMonths={},"
                                + " allowedFutureMonths={}",
                        logSanitizer.sanitize(eventTime),
                        parsedMonth,
                        currentMonth,
                        ALLOWED_PAST_MONTHS,
                        ALLOWED_FUTURE_MONTHS);
                return currentMonth;
            }
            return parsedMonth;
        } catch (DateTimeParseException e) {
            log.warn(
                    "Invalid eventTime format. Fallback to current month. eventTime={}",
                    logSanitizer.sanitize(eventTime));
            return currentMonth;
        }
    }

    private boolean isOutsideAllowedMonthWindow(LocalDate parsedMonth, LocalDate currentMonth) {
        // 과거 1개월까지만 허용하고 미래 월은 허용하지 않는다
        LocalDate minMonth = currentMonth.minusMonths(ALLOWED_PAST_MONTHS);
        LocalDate maxMonth = currentMonth.plusMonths(ALLOWED_FUTURE_MONTHS);
        return parsedMonth.isBefore(minMonth) || parsedMonth.isAfter(maxMonth);
    }

    private boolean persistUsageRecord(
            UsagePersistPayload payload, String eventId, String originEventId) {
        UsageRecord usageRecord =
                UsageRecord.builder()
                        .eventId(originEventId)
                        .familyId(payload.familyId())
                        .customerId(payload.customerId())
                        .bytesUsed(payload.bytesUsed())
                        .appId(payload.appId())
                        .eventTime(resolveEventTime(payload.eventTime()))
                        .build();
        try {
            usageRecordRepository.saveAndFlush(usageRecord);
            return true;
        } catch (DataIntegrityViolationException e) {
            log.info(
                    "Skip duplicated usage_record insert by unique event_id. eventId={},"
                            + " originEventId={}",
                    logSanitizer.sanitize(eventId),
                    logSanitizer.sanitize(originEventId));
            return false;
        }
    }

    private LocalDateTime resolveEventTime(String eventTime) {
        if (eventTime == null || eventTime.isBlank()) {
            return LocalDateTime.now(KST);
        }
        try {
            return OffsetDateTime.parse(eventTime).atZoneSameInstant(KST).toLocalDateTime();
        } catch (DateTimeParseException e) {
            log.warn(
                    "Invalid eventTime format. Fallback to now(KST). eventTime={}",
                    logSanitizer.sanitize(eventTime));
            return LocalDateTime.now(KST);
        }
    }

    private long resolveMonthlyLimitBytes(Long familyId, Long customerId) {
        // 이번 달 row가 없어서 insert할때 과거 row 중 가장 최신 monthlyLimitBytes를 넣음
        return customerQuotaRepository
                .findTopByFamilyIdAndCustomerIdAndDeletedAtIsNullOrderByCurrentMonthDesc(
                        familyId, customerId)
                .map(CustomerQuota::getMonthlyLimitBytes)
                .filter(limit -> limit != null && limit >= 0)
                .orElse(SAFE_DEFAULT_MONTHLY_LIMIT_BYTES);
    }
}
