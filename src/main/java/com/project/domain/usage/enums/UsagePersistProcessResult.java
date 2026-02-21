package com.project.domain.usage.enums;

import java.util.Arrays;

public enum UsagePersistProcessResult {
    BLOCKED_ACCESS("BLOCKED_ACCESS", true, "MANUAL"),
    BLOCKED_TIME("BLOCKED_TIME", true, "TIME_BLOCK"),
    BLOCKED_LIMIT_MONTHLY("BLOCKED_LIMIT_MONTHLY", true, "MONTHLY_LIMIT_EXCEEDED"),
    BLOCKED_FAMILY_QUOTA("BLOCKED_FAMILY_QUOTA", true, "FAMILY_QUOTA_EXCEEDED"),
    NORMAL("NORMAL", false, null),
    WARNING_50("WARNING_50", false, null),
    WARNING_30("WARNING_30", false, null),
    WARNING_10("WARNING_10", false, null),
    ALLOWED("ALLOWED", false, null);

    private final String value;
    private final boolean blocked;
    private final String blockReason;

    UsagePersistProcessResult(String value, boolean blocked, String blockReason) {
        this.value = value;
        this.blocked = blocked;
        this.blockReason = blockReason;
    }

    public static UsagePersistProcessResult from(String rawValue) {
        if (rawValue == null || rawValue.isBlank()) {
            throw new IllegalArgumentException("processResult is null or blank");
        }

        return Arrays.stream(values())
                .filter(result -> result.value.equals(rawValue))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unsupported processResult: " + rawValue));
    }

    public boolean isBlocked() {
        return blocked;
    }

    public String blockReason() {
        return blockReason;
    }
}
