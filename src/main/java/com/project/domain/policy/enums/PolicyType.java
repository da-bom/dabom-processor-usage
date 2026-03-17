package com.project.domain.policy.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum PolicyType {
    MONTHLY_LIMIT("LIMIT:DATA:MONTHLY"),
    TIME_BLOCK("BLOCK:TIME"),
    MANUAL_BLOCK("BLOCK:ACCESS"),
    APP_BLOCK("BLOCK:APP");

    private final String redisKey;
}
