package com.project.domain.family.infra.cache.dto;

import java.time.LocalDate;

import com.project.domain.family.entity.Family;

public record FamilyCacheDto(
        Long id,
        String name,
        Long createdById,
        Long totalQuotaBytes,
        Long usedBytes,
        LocalDate currentMonth) {

    public static FamilyCacheDto from(Family family) {
        return new FamilyCacheDto(
                family.getId(),
                family.getName(),
                family.getCreatedById(),
                family.getTotalQuotaBytes(),
                family.getUsedBytes(),
                family.getCurrentMonth());
    }

    public Family toEntity() {
        return Family.builder()
                .id(id)
                .name(name)
                .createdById(createdById)
                .totalQuotaBytes(totalQuotaBytes)
                .usedBytes(usedBytes)
                .currentMonth(currentMonth)
                .build();
    }
}
