package com.project.domain.usage.service;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.project.domain.usage.enums.UsagePersistProcessResult;
import com.project.domain.usage.service.helper.CustomerQuotaWriter;
import com.project.domain.usage.service.helper.UsagePersistDedupHelper;
import com.project.domain.usage.service.helper.UsagePersistEventValidator;
import com.project.domain.usage.service.helper.UsageRecordWriter;
import com.project.global.common.TimeConstants;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.usage.UsagePersistPayload;
import com.project.global.util.LogSanitizer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class UsagePersistServiceImpl implements UsagePersistService {
    private static final long ALLOWED_PAST_MONTHS = 1;
    private static final long ALLOWED_FUTURE_MONTHS = 0;

    private final UsagePersistEventValidator usagePersistEventValidator;
    private final UsagePersistDedupHelper usagePersistDedupHelper;
    private final UsageRecordWriter usageRecordWriter;
    private final CustomerQuotaWriter customerQuotaWriter;
    private final LogSanitizer logSanitizer;

    // usage-persist 처리의 전체 흐름을 조율
    @Transactional
    public void persist(EventEnvelope<UsagePersistPayload> envelope, String recordKey) {
        UsagePersistPayload payload = envelope.payload();
        String eventId = envelope.eventId();

        // 1) payload 계약 검증
        if (!usagePersistEventValidator.isValidPayload(payload, eventId, recordKey)) {
            return;
        }
        if (payload == null) {
            return;
        }

        String originEventId = payload.originEventId();
        // 2) Redis 기반 단기 중복 차단
        if (usagePersistDedupHelper.isDuplicated(originEventId)) {
            return;
        }

        // 3) 월 기준 계산 + 처리 결과 해석
        LocalDate currentMonth = resolveCurrentMonth(payload.eventTime());
        UsagePersistProcessResult processResult =
                UsagePersistProcessResult.from(payload.processResult());

        // 차단 이벤트는 usage_record를 남기지 않고 차단 상태만 반영한다.
        if (processResult.isBlocked()) {
            customerQuotaWriter.persistBlockedQuota(
                    payload, currentMonth, eventId, originEventId, processResult.blockReason());
            return;
        }

        // usage_record 유니크 충돌이면 이미 처리된 이벤트라 quota 반영도 생략한다.
        if (!usageRecordWriter.persistUsageRecord(payload, eventId, originEventId)) {
            return;
        }

        // 5) 허용 이벤트의 월 누적 반영
        customerQuotaWriter.persistAllowedQuota(payload, currentMonth, eventId, originEventId);
    }

    private LocalDate resolveCurrentMonth(String eventTime) {
        LocalDate currentMonth = LocalDate.now(TimeConstants.ASIA_SEOUL).withDayOfMonth(1);
        if (eventTime == null || eventTime.isBlank()) {
            return currentMonth;
        }
        try {
            LocalDate parsedMonth =
                    OffsetDateTime.parse(eventTime)
                            .atZoneSameInstant(TimeConstants.ASIA_SEOUL)
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
        // 과거 1개월까지만 허용하고 미래 월은 허용하지 않는다.
        LocalDate minMonth = currentMonth.minusMonths(ALLOWED_PAST_MONTHS);
        LocalDate maxMonth = currentMonth.plusMonths(ALLOWED_FUTURE_MONTHS);
        return parsedMonth.isBefore(minMonth) || parsedMonth.isAfter(maxMonth);
    }
}
