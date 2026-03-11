package com.project.domain.usage.service.helper;

import org.springframework.stereotype.Component;

import com.dabom.messaging.kafka.event.dto.usage.UsagePayload;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class UsageEventValidator {
    public boolean isValid(UsagePayload payload, String eventId) {
        // Payload 자체 null 체크
        if (payload == null) {
            log.warn("Usage payload is null. eventId={}", eventId);
            return false;
        }
        // 필수 ID 값 체크 (FamilyId, CustomerId)
        if (payload.familyId() == null || payload.familyId() <= 0) {
            log.warn("Invalid familyId. eventId={}, familyId={}", eventId, payload.familyId());
            return false;
        }
        if (payload.customerId() == null || payload.customerId() <= 0) {
            log.warn(
                    "Invalid customerId. eventId={}, customerId={}", eventId, payload.customerId());
            return false;
        }
        // 사용량 값 체크 (음수, 0)
        if (payload.bytesUsed() == null || payload.bytesUsed() < 0) {
            log.warn("Invalid bytesUsed. eventId={}, bytes={}", eventId, payload.bytesUsed());
            return false;
        }
        return true;
    }
}
