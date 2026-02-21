package com.project.domain.usage.enums;

import java.util.Arrays;

public enum UsagePersistProcessResult {
    MANUAL(true, "MANUAL"),
    TIME_BLOCK(true, "TIME_BLOCK"),
    MONTHLY_LIMIT_EXCEEDED(true, "MONTHLY_LIMIT_EXCEEDED"),
    FAMILY_QUOTA_EXCEEDED(true, "FAMILY_QUOTA_EXCEEDED"),
    ALLOWED(false, null);

    private final boolean blocked;
    private final String blockReason;

    UsagePersistProcessResult(boolean blocked, String blockReason) {
        this.blocked = blocked;
        this.blockReason = blockReason;
    }

    public static UsagePersistProcessResult from(String rawValue) {
        if (rawValue == null || rawValue.isBlank()) {
            throw new IllegalArgumentException("processResult is null or blank");
        }

        return Arrays.stream(values())
                .filter(result -> result.name().equals(rawValue))
                .findFirst()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "Unsupported processResult: " + rawValue));
    }

    public boolean isBlocked() {
        return blocked;
    }

    public String blockReason() {
        return blockReason;
    }
}
