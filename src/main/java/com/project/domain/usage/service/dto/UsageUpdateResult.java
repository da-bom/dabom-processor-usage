package com.project.domain.usage.service.dto;

public record UsageUpdateResult(
        long totalUsed,
        long remaining,
        String status,
        long monthlyUsed,
        double userRatio,
        long monthlyLimit,
        // 현재 상태에 대해 알림 발행이 필요한지 여부다.
        boolean shouldNotify,
        // usage-event duplicate 여부다.
        boolean duplicate) {}
