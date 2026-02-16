package com.project.domain.usage.service.dto;

public record UsageUpdateResult(
        long totalUsed,
        long remaining,
        String status,
        long monthlyUsed,
        double userRatio,
        long monthlyLimit) {}
