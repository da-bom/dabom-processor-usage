package com.project.domain.policy.infra.cache.dto;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import com.project.domain.policy.enums.PolicyType;

public final class PolicyConstraintRedisHash {
    private final Map<String, String> hash;

    private PolicyConstraintRedisHash(Map<String, String> hash) {
        this.hash = hash;
    }

    public static PolicyConstraintRedisHash create() {
        return new PolicyConstraintRedisHash(new LinkedHashMap<>());
    }

    public void putMonthlyLimit(long limitBytes) {
        put(PolicyType.MONTHLY_LIMIT.getRedisKey(), String.valueOf(limitBytes));
    }

    public void putTimeBlockRange(String timeRange) {
        put(PolicyType.TIME_BLOCK.getRedisKey(), timeRange);
    }

    public void putManualBlock() {
        put(PolicyType.MANUAL_BLOCK.getRedisKey(), "1");
    }

    public void putBlockedApp(String appId) {
        put(PolicyType.APP_BLOCK.getRedisKey() + ":" + appId.trim().toLowerCase(Locale.ROOT), "1");
    }

    public Map<String, String> toMap() {
        return hash;
    }

    private void put(String key, String value) {
        hash.put(key, value);
    }
}
