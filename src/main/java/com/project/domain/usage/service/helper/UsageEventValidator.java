package com.project.domain.usage.service.helper;

import org.springframework.stereotype.Component;

import com.dabom.messaging.kafka.event.dto.usage.UsagePayload;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class UsageEventValidator {

    // usage-events payload의 기본 형식을 검증한다.
    public boolean isValid(UsagePayload payload, String eventId) {
        if (payload == null) {
            log.warn("Usage payload is null. eventId={}", eventId);
            return false;
        }
        if (payload.familyId() == null || payload.familyId() <= 0) {
            log.warn("Invalid familyId. eventId={}, familyId={}", eventId, payload.familyId());
            return false;
        }
        if (payload.customerId() == null || payload.customerId() <= 0) {
            log.warn(
                    "Invalid customerId. eventId={}, customerId={}", eventId, payload.customerId());
            return false;
        }
        // 사용량은 양수만 허용한다.
        if (payload.bytesUsed() == null || payload.bytesUsed() <= 0) {
            log.warn("Invalid bytesUsed. eventId={}, bytes={}", eventId, payload.bytesUsed());
            return false;
        }
        return true;
    }
}
