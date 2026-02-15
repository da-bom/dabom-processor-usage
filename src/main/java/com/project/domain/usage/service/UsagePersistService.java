package com.project.domain.usage.service;

import java.time.Duration;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.project.domain.customer.entity.CustomerQuota;
import com.project.domain.customer.repository.CustomerQuotaRepository;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.usage.UsagePersistPayload;
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
    private static final Pattern LOG_DANGEROUS_PATTERN = Pattern.compile("[\\r\\n\\t]");
    private static final int MAX_LOG_VALUE_LENGTH = 128;
    private static final String USAGE_PERSIST_LOG_SUFFIX =
            " familyId={}, customerId={}, bytesUsed={}, currentMonth={}";
    private static final long ALLOWED_PAST_MONTHS = 1;
    private static final long ALLOWED_FUTURE_MONTHS = 0;
    private static final long SAFE_DEFAULT_MONTHLY_LIMIT_BYTES = 0L;

    private final RedisTemplate<String, String> familyStringRedisTemplate;
    private final RedisKeyGenerator redisKeyGenerator;
    private final UsagePersistEventValidator usagePersistEventValidator;
    private final CustomerQuotaRepository customerQuotaRepository;

    @Value("${app.kafka.dedup.usage-persist-ttl-seconds}")
    private long usagePersistDedupTtlSeconds;

    @Transactional
    public void persist(EventEnvelope<UsagePersistPayload> envelope, String recordKey) {
        UsagePersistPayload payload = envelope.payload();
        String eventId = envelope.eventId();
        String safeEventId = sanitizeForLog(eventId);
        String safeOriginEventId = sanitizeForLog(payload == null ? null : payload.originEventId());

        if (!usagePersistEventValidator.isValidPayload(payload, eventId, recordKey)) {
            return;
        }

        if (isDuplicated(payload.originEventId(), safeOriginEventId)) {
            return;
        }

        LocalDate currentMonth = resolveCurrentMonth(payload.eventTime());
        int updatedRows =
                customerQuotaRepository.incrementMonthlyUsedBytes(
                        payload.familyId(),
                        payload.customerId(),
                        currentMonth,
                        payload.bytesUsed());
        if (updatedRows > 0) {
            log.info(
                    "Persisted usage to existing quota row. eventId={}, originEventId={},"
                            + USAGE_PERSIST_LOG_SUFFIX,
                    safeEventId,
                    safeOriginEventId,
                    payload.familyId(),
                    payload.customerId(),
                    payload.bytesUsed(),
                    currentMonth);
            return;
        }

        long monthlyLimitBytes = resolveMonthlyLimitBytes(payload.familyId(), payload.customerId());
        CustomerQuota customerQuota =
                CustomerQuota.builder()
                        .familyId(payload.familyId())
                        .customerId(payload.customerId())
                        .monthlyUsedBytes(payload.bytesUsed())
                        .currentMonth(currentMonth)
                        .monthlyLimitBytes(monthlyLimitBytes)
                        .isBlocked(false)
                        .blockReason(null)
                        .build();
        try {
            customerQuotaRepository.saveAndFlush(customerQuota);
        } catch (DataIntegrityViolationException e) {
            int retriedRows =
                    customerQuotaRepository.incrementMonthlyUsedBytes(
                            payload.familyId(),
                            payload.customerId(),
                            currentMonth,
                            payload.bytesUsed());
            if (retriedRows > 0) {
                log.info(
                        "Recovered from concurrent insert race. eventId={}, originEventId={},"
                                + USAGE_PERSIST_LOG_SUFFIX,
                        safeEventId,
                        safeOriginEventId,
                        payload.familyId(),
                        payload.customerId(),
                        payload.bytesUsed(),
                        currentMonth);
                return;
            }
            throw e;
        }

        log.info(
                "Persisted usage by creating quota row. eventId={}, originEventId={},"
                        + USAGE_PERSIST_LOG_SUFFIX,
                safeEventId,
                safeOriginEventId,
                payload.familyId(),
                payload.customerId(),
                payload.bytesUsed(),
                currentMonth);
    }

    private boolean isDuplicated(String originEventId, String safeOriginEventId) {
        String dedupKey = redisKeyGenerator.generateUsagePersistEventDedupKey(originEventId);
        try {
            Boolean firstSeen =
                    familyStringRedisTemplate
                            .opsForValue()
                            .setIfAbsent(
                                    dedupKey,
                                    DEDUP_FLAG,
                                    Duration.ofSeconds(usagePersistDedupTtlSeconds));
            if (!Boolean.TRUE.equals(firstSeen)) {
                log.info(
                        "Skip duplicated usage-persist event. originEventId={}", safeOriginEventId);
                return true;
            }
            return false;
        } catch (DataAccessException e) {
            log.warn(
                    "Redis dedup failed. Continue DB persist for availability. originEventId={}",
                    safeOriginEventId,
                    e);
            return false;
        }
    }

    private LocalDate resolveCurrentMonth(String eventTime) {
        LocalDate currentMonth = LocalDate.now(KST).withDayOfMonth(1);
        if (eventTime == null || eventTime.isBlank()) {
            return currentMonth;
        }
        try {
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
                        sanitizeForLog(eventTime),
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
                    sanitizeForLog(eventTime));
            return currentMonth;
        }
    }

    private boolean isOutsideAllowedMonthWindow(LocalDate parsedMonth, LocalDate currentMonth) {
        LocalDate minMonth = currentMonth.minusMonths(ALLOWED_PAST_MONTHS);
        LocalDate maxMonth = currentMonth.plusMonths(ALLOWED_FUTURE_MONTHS);
        return parsedMonth.isBefore(minMonth) || parsedMonth.isAfter(maxMonth);
    }

    private String sanitizeForLog(String raw) {
        if (raw == null) {
            return "null";
        }
        String sanitized = LOG_DANGEROUS_PATTERN.matcher(raw).replaceAll("_");
        if (sanitized.length() > MAX_LOG_VALUE_LENGTH) {
            return sanitized.substring(0, MAX_LOG_VALUE_LENGTH) + "...";
        }
        return sanitized;
    }

    private long resolveMonthlyLimitBytes(Long familyId, Long customerId) {
        return customerQuotaRepository
                .findTopByFamilyIdAndCustomerIdAndDeletedAtIsNullOrderByCurrentMonthDesc(
                        familyId, customerId)
                .map(CustomerQuota::getMonthlyLimitBytes)
                .filter(limit -> limit != null && limit >= 0)
                .orElse(SAFE_DEFAULT_MONTHLY_LIMIT_BYTES);
    }
}
