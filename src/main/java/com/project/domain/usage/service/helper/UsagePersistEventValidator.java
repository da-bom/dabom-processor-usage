package com.project.domain.usage.service.helper;

import org.springframework.stereotype.Component;

import com.project.domain.usage.enums.UsagePersistProcessResult;
import com.project.domain.usage.service.dto.UsagePersistPayload;
import com.project.global.util.LogSanitizer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class UsagePersistEventValidator {

    private final LogSanitizer logSanitizer;

    public boolean isValidPayload(UsagePersistPayload payload, String eventId, String recordKey) {
        if (payload == null) {
            log.warn("usage payload is null. recordKey={}", recordKey);
            return false;
        }

        if (payload.originEventId() == null || payload.originEventId().isBlank()) {
            log.warn(
                    "usage originEventId is empty. eventId={}, familyId={}, customerId={}",
                    logSanitizer.sanitize(eventId),
                    payload.familyId(),
                    payload.customerId());
            return false;
        }

        if (payload.familyId() == null || payload.customerId() == null) {
            log.warn(
                    "usage family/customer is invalid. eventId={}, originEventId={},"
                            + " familyId={}, customerId={}",
                    logSanitizer.sanitize(eventId),
                    logSanitizer.sanitize(payload.originEventId()),
                    payload.familyId(),
                    payload.customerId());
            return false;
        }

        if (payload.bytesUsed() == null || payload.bytesUsed() <= 0) {
            log.warn(
                    "usage bytesUsed is invalid. eventId={}, originEventId={}," + " bytesUsed={}",
                    logSanitizer.sanitize(eventId),
                    logSanitizer.sanitize(payload.originEventId()),
                    payload.bytesUsed());
            return false;
        }

        try {
            UsagePersistProcessResult.from(payload.processResult());
        } catch (IllegalArgumentException e) {
            log.warn(
                    "usage processResult is invalid. eventId={}, originEventId={},"
                            + " processResult={}",
                    logSanitizer.sanitize(eventId),
                    logSanitizer.sanitize(payload.originEventId()),
                    logSanitizer.sanitize(payload.processResult()));
            return false;
        }

        return true;
    }
}
