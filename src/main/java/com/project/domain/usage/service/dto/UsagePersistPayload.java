package com.project.domain.usage.service.dto;

// usage-events 처리 결과를 DB 정산 로직으로 넘길 때 사용하는 내부 DTO다.
public record UsagePersistPayload(
        String originEventId,
        Long familyId,
        Long customerId,
        Long bytesUsed,
        String appId,
        String processResult,
        String eventTime) {}
