package com.project.global.event.dto.usage;

public record UsageRealtimePayload(
        Long familyId,
        Long customerId,
        Long totalUsedBytes, // 가족 전체 사용량
        Long totalLimitBytes, // 가족 전체 할당량
        Long remainingBytes, // 가족 잔여 데이터
        Double usedPercent, // 가족 데이터 소진율 (%)
        Long monthlyUsedBytes, // 개인 월간 사용량
        Double userUsagePercent, // 개인 기여도 (%)
        Long monthlyLimitBytes) {}
