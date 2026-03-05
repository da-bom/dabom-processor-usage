package com.project.domain.usage.service.helper;

import java.time.LocalDate;

import org.springframework.stereotype.Service;

import com.project.domain.family.repository.FamilyRepository;
import com.project.global.util.LogSanitizer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class FamilyUsageWriter {

    private final FamilyRepository familyRepository;
    private final LogSanitizer logSanitizer;

    // 허용 이벤트의 bytesUsed를 family.used_bytes에 누적 반영한다.
    public void updateFamilyUsedBytes(
            Long familyId,
            LocalDate eventMonth,
            Long bytesUsed,
            String eventId,
            String originEventId) {
        int updatedRows =
                familyRepository.updateUsedBytesByEventMonth(familyId, eventMonth, bytesUsed);
        if (updatedRows > 0) {
            return;
        }

        log.warn(
                "Failed to update family.used_bytes. eventId={}, originEventId={}, familyId={},"
                        + " eventMonth={}, bytesUsed={}",
                logSanitizer.sanitize(eventId),
                logSanitizer.sanitize(originEventId),
                familyId,
                eventMonth,
                bytesUsed);
        throw new IllegalStateException("Failed to update family used bytes");
    }
}
