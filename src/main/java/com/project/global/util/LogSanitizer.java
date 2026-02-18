package com.project.global.util;

import java.util.regex.Pattern;

import org.springframework.stereotype.Component;

@Component
public class LogSanitizer {
    private static final Pattern LOG_DANGEROUS_PATTERN = Pattern.compile("[\\r\\n\\t]");
    private static final int MAX_LOG_VALUE_LENGTH = 128;

    public String sanitize(String raw) {
        if (raw == null) {
            return "null";
        }
        String sanitized = LOG_DANGEROUS_PATTERN.matcher(raw).replaceAll("_");
        if (sanitized.length() > MAX_LOG_VALUE_LENGTH) {
            return sanitized.substring(0, MAX_LOG_VALUE_LENGTH) + "...";
        }
        return sanitized;
    }
}
