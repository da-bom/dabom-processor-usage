package com.project.domain.usage.service.helper;

import java.time.LocalDate;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import com.project.domain.customer.entity.CustomerQuota;
import com.project.domain.customer.repository.CustomerQuotaRepository;
import com.project.domain.usage.service.dto.UsagePersistPayload;
import com.project.global.util.LogSanitizer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class CustomerQuotaWriter {
    private static final String USAGE_PERSIST_LOG_SUFFIX =
            " familyId={}, customerId={}, bytesUsed={}, currentMonth={}";

    private final CustomerQuotaRepository customerQuotaRepository;
    private final LogSanitizer logSanitizer;

    // 허용 이벤트: monthly_used_bytes를 누적 반영한다.
    public void persistAllowedQuota(
            UsagePersistPayload payload,
            LocalDate currentMonth,
            String eventId,
            String originEventId) {
        if (tryUpdateExistingQuota(payload, currentMonth, eventId, originEventId)) {
            return;
        }
        createAllowedQuotaRow(payload, currentMonth, eventId, originEventId);
    }

    // 차단 이벤트: 사용량 누적 없이 차단 상태만 반영한다.
    public void persistBlockedQuota(
            UsagePersistPayload payload,
            LocalDate currentMonth,
            String eventId,
            String originEventId,
            String blockReason) {
        if (tryUpdateBlockedQuota(payload, currentMonth, eventId, originEventId, blockReason)) {
            return;
        }
        createBlockedQuotaRow(payload, currentMonth, eventId, originEventId, blockReason);
    }

    private boolean tryUpdateExistingQuota(
            UsagePersistPayload payload,
            LocalDate currentMonth,
            String eventId,
            String originEventId) {
        // 이미 월 row가 있으면 update 1회로 종료한다.
        int updatedRows =
                customerQuotaRepository.incrementMonthlyUsedBytes(
                        payload.familyId(),
                        payload.customerId(),
                        currentMonth,
                        payload.bytesUsed(),
                        false,
                        null);
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

    private void createAllowedQuotaRow(
            UsagePersistPayload payload,
            LocalDate currentMonth,
            String eventId,
            String originEventId) {
        // 월 row가 없으면 insert로 생성한다.
        Long monthlyLimitBytes = resolveMonthlyLimitBytes(payload.familyId(), payload.customerId());
        CustomerQuota customerQuota =
                buildCustomerQuota(
                        payload.familyId(),
                        payload.customerId(),
                        currentMonth,
                        monthlyLimitBytes,
                        payload.bytesUsed(),
                        false,
                        null);
        try {
            customerQuotaRepository.saveAndFlush(customerQuota);
        } catch (DataIntegrityViolationException e) {
            // 동시 insert 경합이면 update 재시도로 복구한다.
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
        int retriedRows =
                customerQuotaRepository.incrementMonthlyUsedBytes(
                        payload.familyId(),
                        payload.customerId(),
                        currentMonth,
                        payload.bytesUsed(),
                        false,
                        null);
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

    private boolean tryUpdateBlockedQuota(
            UsagePersistPayload payload,
            LocalDate currentMonth,
            String eventId,
            String originEventId,
            String blockReason) {
        // 차단 이벤트는 is_blocked/block_reason만 갱신한다.
        int updatedRows =
                customerQuotaRepository.updateBlockState(
                        payload.familyId(), payload.customerId(), currentMonth, true, blockReason);
        if (updatedRows <= 0) {
            return false;
        }

        log.info(
                "Persisted blocked state to existing quota row. eventId={}, originEventId={},"
                        + " familyId={}, customerId={}, blockReason={}, currentMonth={}",
                logSanitizer.sanitize(eventId),
                logSanitizer.sanitize(originEventId),
                payload.familyId(),
                payload.customerId(),
                blockReason,
                currentMonth);
        return true;
    }

    private void createBlockedQuotaRow(
            UsagePersistPayload payload,
            LocalDate currentMonth,
            String eventId,
            String originEventId,
            String blockReason) {
        // 월 row가 없으면 차단 상태 row를 생성한다.
        Long monthlyLimitBytes = resolveMonthlyLimitBytes(payload.familyId(), payload.customerId());
        CustomerQuota customerQuota =
                buildCustomerQuota(
                        payload.familyId(),
                        payload.customerId(),
                        currentMonth,
                        monthlyLimitBytes,
                        0L,
                        true,
                        blockReason);
        try {
            customerQuotaRepository.saveAndFlush(customerQuota);
        } catch (DataIntegrityViolationException e) {
            if (tryUpdateBlockedQuota(payload, currentMonth, eventId, originEventId, blockReason)) {
                return;
            }
            throw e;
        }

        log.info(
                "Persisted blocked state by creating quota row. eventId={}, originEventId={},"
                        + " familyId={}, customerId={}, blockReason={}, currentMonth={}",
                logSanitizer.sanitize(eventId),
                logSanitizer.sanitize(originEventId),
                payload.familyId(),
                payload.customerId(),
                blockReason,
                currentMonth);
    }

    private CustomerQuota buildCustomerQuota(
            Long familyId,
            Long customerId,
            LocalDate currentMonth,
            Long monthlyLimitBytes,
            Long monthlyUsedBytes,
            boolean isBlocked,
            String blockReason) {
        return CustomerQuota.builder()
                .familyId(familyId)
                .customerId(customerId)
                .monthlyUsedBytes(monthlyUsedBytes)
                .currentMonth(currentMonth)
                .monthlyLimitBytes(monthlyLimitBytes)
                .isBlocked(isBlocked)
                .blockReason(blockReason)
                .build();
    }

    private Long resolveMonthlyLimitBytes(Long familyId, Long customerId) {
        // 직전 월 limit를 이어받고, 없으면 NULL(무제한)로 둔다.
        return customerQuotaRepository
                .findTopByFamilyIdAndCustomerIdAndDeletedAtIsNullOrderByCurrentMonthDesc(
                        familyId, customerId)
                .map(
                        quota -> {
                            Long limit = quota.getMonthlyLimitBytes();
                            if (limit == null || limit >= 0) {
                                return limit;
                            }
                            log.warn(
                                    "Invalid previous monthlyLimitBytes. Fallback to"
                                            + " null(unlimited). familyId={}, customerId={},"
                                            + " monthlyLimitBytes={}",
                                    familyId,
                                    customerId,
                                    limit);
                            return null;
                        })
                .orElse(null);
    }
}
