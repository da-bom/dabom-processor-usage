package com.project.domain.usage.infra.cache.dto;

import java.time.LocalDateTime;
import java.util.Map;

import com.project.domain.usage.service.dto.FamilyInfo;

public record FamilyInfoRedisHash(String name, String totalQuota, String createdAt) {
    public static FamilyInfoRedisHash from(Map<Object, Object> hash) {
        if (hash == null || hash.isEmpty()) {
            return null;
        }

        // 캐스팅
        String name = asString(hash.get("name"));
        String totalQuota = asString(hash.get("totalQuota"));
        String createdAt = asString(hash.get("createdAt"));

        // 필수 필드 누락 방어
        if (name == null || totalQuota == null || createdAt == null) {
            return null;
        }

        return new FamilyInfoRedisHash(name, totalQuota, createdAt);
    }

    private static String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    public FamilyInfo toDomain(long familyId) {
        return new FamilyInfo(
                familyId, name, Long.parseLong(totalQuota), LocalDateTime.parse(createdAt));
    }

    public static Map<String, String> toHash(FamilyInfo info) {
        return Map.of(
                "name", info.name(),
                "totalQuota", String.valueOf(info.totalQuotaBytes()),
                "createdAt", info.createdAt().toString());
    }
}
