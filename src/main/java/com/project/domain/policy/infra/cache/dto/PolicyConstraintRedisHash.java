package com.project.domain.policy.infra.cache.dto;

import java.util.LinkedHashMap;
import java.util.Map;

import com.project.domain.policy.constant.PolicyConstraintKeyConstants;

public final class PolicyConstraintRedisHash {
    private final Map<String, String> hash;

    private PolicyConstraintRedisHash(Map<String, String> hash) {
        this.hash = hash;
    }

    public static PolicyConstraintRedisHash create() {
        return new PolicyConstraintRedisHash(new LinkedHashMap<>());
    }

    public void putMonthlyLimit(long limitBytes, long version) {
        putWithVersion(
                PolicyConstraintKeyConstants.LIMIT_DATA_MONTHLY,
                String.valueOf(limitBytes),
                version);
    }

    public void putTimeBlockRange(String timeRange, long version) {
        putWithVersion(PolicyConstraintKeyConstants.BLOCK_TIME, timeRange, version);
    }

    public void putManualBlock(long version) {
        putWithVersion(PolicyConstraintKeyConstants.BLOCK_ACCESS, "1", version);
    }

    public void putBlockedApp(String appId, long version) {
        putWithVersion(PolicyConstraintKeyConstants.BLOCK_APP_PREFIX + appId, "1", version);
    }

    public Map<String, String> toMap() {
        return hash;
    }

    private void putWithVersion(String key, String value, long version) {
        hash.put(key, value);
        hash.put(buildVersionField(key), String.valueOf(version));
    }

    private String buildVersionField(String key) {
        return PolicyConstraintKeyConstants.VERSION_FIELD_PREFIX + key;
    }
}
