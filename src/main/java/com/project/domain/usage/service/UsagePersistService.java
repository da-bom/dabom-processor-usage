package com.project.domain.usage.service;

import java.time.Duration;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
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
    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

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

        if (!usagePersistEventValidator.isValidPayload(payload, eventId, recordKey)) {
            return;
        }

        if (isDuplicated(payload.originEventId())) {
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
                            + " familyId={}, customerId={}, bytesUsed={}, currentMonth={}",
                    eventId,
                    payload.originEventId(),
                    payload.familyId(),
                    payload.customerId(),
                    payload.bytesUsed(),
                    currentMonth);
            return;
        }

        CustomerQuota customerQuota =
                CustomerQuota.builder()
                        .familyId(payload.familyId())
                        .customerId(payload.customerId())
                        .monthlyUsedBytes(payload.bytesUsed())
                        .currentMonth(currentMonth)
                        .monthlyLimitBytes(null)
                        .isBlocked(false)
                        .blockReason(null)
                        .build();
        customerQuotaRepository.save(customerQuota);
        log.info(
                "Persisted usage by creating quota row. eventId={}, originEventId={},"
                        + " familyId={}, customerId={}, bytesUsed={}, currentMonth={}",
                eventId,
                payload.originEventId(),
                payload.familyId(),
                payload.customerId(),
                payload.bytesUsed(),
                currentMonth);
    }

    private boolean isDuplicated(String originEventId) {
        String dedupKey = redisKeyGenerator.generateUsagePersistEventDedupKey(originEventId);
        try {
            Boolean firstSeen =
                    familyStringRedisTemplate
                            .opsForValue()
                            .setIfAbsent(
                                    dedupKey, "1", Duration.ofSeconds(usagePersistDedupTtlSeconds));
            if (!Boolean.TRUE.equals(firstSeen)) {
                log.info("Skip duplicated usage-persist event. originEventId={}", originEventId);
                return true;
            }
            return false;
        } catch (DataAccessException e) {
            log.warn(
                    "Redis dedup failed. Continue DB persist for availability. originEventId={}",
                    originEventId,
                    e);
            return false;
        }
    }

    private LocalDate resolveCurrentMonth(String eventTime) {
        if (eventTime == null || eventTime.isBlank()) {
            return LocalDate.now(KST).withDayOfMonth(1);
        }
        try {
            return OffsetDateTime.parse(eventTime)
                    .atZoneSameInstant(KST)
                    .toLocalDate()
                    .withDayOfMonth(1);
        } catch (DateTimeParseException e) {
            log.warn(
                    "Invalid eventTime format. Fallback to current month. eventTime={}", eventTime);
            return LocalDate.now(KST).withDayOfMonth(1);
        }
    }
}
