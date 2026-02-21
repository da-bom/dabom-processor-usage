package com.project.domain.usage.service.dto;

import java.time.LocalDateTime;

public record FamilyInfo(
        long familyId, String name, long totalQuotaBytes, LocalDateTime createdAt) {}
