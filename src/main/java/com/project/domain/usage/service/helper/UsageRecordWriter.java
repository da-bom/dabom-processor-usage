package com.project.domain.usage.service.helper;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import com.project.domain.usage.entity.UsageRecord;
import com.project.domain.usage.repository.UsageRecordRepository;
import com.project.global.common.TimeConstants;
import com.project.global.event.dto.usage.UsagePersistPayload;
import com.project.global.util.LogSanitizer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class UsageRecordWriter {

    private final UsageRecordRepository usageRecordRepository;
    private final LogSanitizer logSanitizer;

    // usage_record 저장 성공 여부를 반환한다.
    // false는 유니크 충돌(이미 저장된 이벤트) 케이스다.
    public boolean persistUsageRecord(
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
            // event_id 유니크 충돌은 이미 처리된 이벤트로 본다.
            log.info(
                    "Skip duplicated usage_record insert by unique event_id. eventId={},"
                            + " originEventId={}",
                    logSanitizer.sanitize(eventId),
                    logSanitizer.sanitize(originEventId));
            return false;
        }
    }

    private LocalDateTime resolveEventTime(String eventTime) {
        // event_time이 없거나 파싱 실패면 현재 KST 시각을 사용한다.
        if (eventTime == null || eventTime.isBlank()) {
            return LocalDateTime.now(TimeConstants.ASIA_SEOUL);
        }
        try {
            return OffsetDateTime.parse(eventTime)
                    .atZoneSameInstant(TimeConstants.ASIA_SEOUL)
                    .toLocalDateTime();
        } catch (DateTimeParseException e) {
            log.warn(
                    "Invalid eventTime format. Fallback to now(KST). eventTime={}",
                    logSanitizer.sanitize(eventTime));
            return LocalDateTime.now(TimeConstants.ASIA_SEOUL);
        }
    }
}
