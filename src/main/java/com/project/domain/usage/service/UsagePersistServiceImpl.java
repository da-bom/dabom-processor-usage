package com.project.domain.usage.service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.dabom.messaging.kafka.event.dto.usage.UsagePayload;
import com.project.domain.family.repository.FamilyMemberRepository;
import com.project.domain.usage.enums.UsagePersistProcessResult;
import com.project.domain.usage.service.dto.UsagePersistPayload;
import com.project.domain.usage.service.helper.CustomerQuotaWriter;
import com.project.domain.usage.service.helper.FamilyQuotaWriter;
import com.project.domain.usage.service.helper.UsagePersistEventValidator;
import com.project.domain.usage.service.helper.UsageRecordWriter;
import com.project.global.common.TimeConstants;
import com.project.global.util.LogSanitizer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class UsagePersistServiceImpl implements UsagePersistService {
    private static final long ALLOWED_PAST_MONTHS = 1;
    private static final long ALLOWED_FUTURE_MONTHS = 0;

    private final FamilyMemberRepository familyMemberRepository;
    private final UsagePersistEventValidator usagePersistEventValidator;
    private final UsageRecordWriter usageRecordWriter;
    private final CustomerQuotaWriter customerQuotaWriter;
    private final FamilyQuotaWriter familyQuotaWriter;
    private final LogSanitizer logSanitizer;

    // usage-events 처리 결과를 DB 정산으로 직접 반영한다.
    @Override
    @Transactional
    public void persistFromUsageEvent(
            String eventId, String eventTime, UsagePayload usagePayload, String processResult) {
        UsagePersistPayload payload =
                new UsagePersistPayload(
                        eventId,
                        usagePayload.familyId(),
                        usagePayload.customerId(),
                        usagePayload.bytesUsed(),
                        usagePayload.appId(),
                        processResult,
                        eventTime);

        persistInternal(payload, eventId, String.valueOf(usagePayload.familyId()));
    }

    // 검증 후 usage_record, quota, family usage를 정산 규칙에 맞게 반영한다.
    private void persistInternal(UsagePersistPayload payload, String eventId, String recordKey) {
        if (!usagePersistEventValidator.isValidPayload(payload, eventId, recordKey)) {
            return;
        }

        String originEventId = payload.originEventId();

        // 가족-구성원 관계가 유효하지 않으면 정산을 중단한다.
        if (!isValidFamilyMember(payload.familyId(), payload.customerId())) {
            log.warn(
                    "Skip usage persistence due to invalid family-customer relation. eventId={},"
                            + " originEventId={}, familyId={}, customerId={}",
                    logSanitizer.sanitize(eventId),
                    logSanitizer.sanitize(originEventId),
                    payload.familyId(),
                    payload.customerId());
            return;
        }

        LocalDate currentMonth = resolveCurrentMonth(payload.eventTime());
        UsagePersistProcessResult result = UsagePersistProcessResult.from(payload.processResult());

        // 차단 이벤트는 usage_record를 만들지 않고 차단 상태만 반영한다.
        if (result.isBlocked()) {
            customerQuotaWriter.persistBlockedQuota(
                    payload, currentMonth, eventId, originEventId, result.blockReason());
            return;
        }

        // usage_record가 이미 있으면 이미 처리된 이벤트로 본다.
        if (!usageRecordWriter.persistUsageRecord(payload, eventId, originEventId)) {
            return;
        }

        customerQuotaWriter.persistAllowedQuota(payload, currentMonth, eventId, originEventId);
        familyQuotaWriter.persistAllowedQuota(
                payload.familyId(), currentMonth, payload.bytesUsed(), eventId, originEventId);
    }

    // 현재 가족의 유효한 구성원인지 확인한다.
    private boolean isValidFamilyMember(Long familyId, Long customerId) {
        return familyMemberRepository.existsByFamilyIdAndCustomerIdAndDeletedAtIsNull(
                familyId, customerId);
    }

    // 이벤트 시각을 정산 월로 변환하고 이상 값이면 현재 월로 보정한다.
    private LocalDate resolveCurrentMonth(String eventTime) {
        LocalDate currentMonth = LocalDate.now(TimeConstants.ASIA_SEOUL).withDayOfMonth(1);
        if (eventTime == null || eventTime.isBlank()) {
            return currentMonth;
        }
        try {
            LocalDate parsedMonth =
                    LocalDateTime.parse(eventTime)
                            .atZone(TimeConstants.ASIA_SEOUL)
                            .toLocalDate()
                            .withDayOfMonth(1);

            // 허용 범위를 벗어난 월은 잘못된 이벤트 시각으로 보고 현재 월로 보정한다.
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

    // 정산 허용 범위를 벗어난 월인지 확인한다.
    private boolean isOutsideAllowedMonthWindow(LocalDate parsedMonth, LocalDate currentMonth) {
        LocalDate minMonth = currentMonth.minusMonths(ALLOWED_PAST_MONTHS);
        LocalDate maxMonth = currentMonth.plusMonths(ALLOWED_FUTURE_MONTHS);
        return parsedMonth.isBefore(minMonth) || parsedMonth.isAfter(maxMonth);
    }
}
