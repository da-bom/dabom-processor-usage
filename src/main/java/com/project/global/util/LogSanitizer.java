package com.project.global.util;

import java.util.regex.Pattern;

import org.springframework.stereotype.Component;

@Component
public class LogSanitizer {
    // 로그 인젝션/멀티라인 로그 깨짐 방지를 위해 제어문자를 치환
    private static final Pattern LOG_DANGEROUS_PATTERN = Pattern.compile("[\\r\\n\\t]");
    // 로그 필드 과다 길이로 인한 가독성 저하를 막기 위한 최대 길이
    private static final int MAX_LOG_VALUE_LENGTH = 128;

    public String sanitize(String raw) {
        // null 값은 문자열 "null"로 통일
        if (raw == null) {
            return "null";
        }

        // 줄바꿈/탭 문자를 언더스코어로 치환해 단일 라인 로그를 유지
        String sanitized = LOG_DANGEROUS_PATTERN.matcher(raw).replaceAll("_");

        // 지정 길이를 넘으면 접미사(...)를 붙여 잘라냄
        if (sanitized.length() > MAX_LOG_VALUE_LENGTH) {
            return sanitized.substring(0, MAX_LOG_VALUE_LENGTH) + "...";
        }
        return sanitized;
    }
}
