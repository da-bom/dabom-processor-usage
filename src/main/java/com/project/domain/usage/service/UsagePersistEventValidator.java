package com.project.domain.usage.service;

import org.springframework.stereotype.Component;

import com.project.global.event.dto.usage.UsagePersistPayload;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class UsagePersistEventValidator {

    public boolean isValidPayload(UsagePersistPayload payload, String eventId, String recordKey) {
        if (payload == null) {
            log.warn("usage-persist payload is null. recordKey={}", recordKey);
            return false;
        }

        if (payload.originEventId() == null || payload.originEventId().isBlank()) {
            log.warn(
                    "usage-persist originEventId is empty. eventId={}, familyId={}, customerId={}",
                    eventId,
                    payload.familyId(),
                    payload.customerId());
            return false;
        }

        if (payload.familyId() == null || payload.customerId() == null) {
            log.warn(
                    "usage-persist family/customer is invalid. eventId={}, originEventId={},"
                            + " familyId={}, customerId={}",
                    eventId,
                    payload.originEventId(),
                    payload.familyId(),
                    payload.customerId());
            return false;
        }

        if (payload.bytesUsed() == null || payload.bytesUsed() <= 0) {
            log.warn(
                    "usage-persist bytesUsed is invalid. eventId={}, originEventId={},"
                            + " bytesUsed={}",
                    eventId,
                    payload.originEventId(),
                    payload.bytesUsed());
            return false;
        }

        return true;
    }
}
