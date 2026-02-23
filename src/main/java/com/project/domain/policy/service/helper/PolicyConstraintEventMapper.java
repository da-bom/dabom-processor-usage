package com.project.domain.policy.service.helper;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.domain.policy.constant.PolicyConstraintKeyConstants;
import com.project.domain.policy.constant.PolicyRuleKeyConstants;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class PolicyConstraintEventMapper {
    private static final Pattern HHMM_PATTERN = Pattern.compile("^\\d{4}$");
    private static final Pattern HHMM_RANGE_PATTERN = Pattern.compile("^\\d{4}-\\d{4}$");

    private final ObjectMapper objectMapper;

    public String normalizeValue(String policyKey, String newValue) {
        if (isBlank(newValue)) {
            return null;
        }
        return switch (policyKey) {
            case PolicyConstraintKeyConstants.LIMIT_DATA_MONTHLY -> normalizeMonthlyLimit(newValue);
            case PolicyConstraintKeyConstants.BLOCK_TIME -> normalizeTimeBlock(newValue);
            case PolicyConstraintKeyConstants.BLOCK_ACCESS -> normalizeManualBlock(newValue);
            case PolicyConstraintKeyConstants.BLOCK_APP -> normalizeAppBlock(newValue);
            default -> throw new IllegalArgumentException("Unsupported policy key: " + policyKey);
        };
    }

    private String normalizeMonthlyLimit(String newValue) {
        if (looksLikeJson(newValue)) {
            return String.valueOf(toPositiveLong(newValue, "newValue"));
        }

        Map<String, Object> rules = parseRulesJson(newValue);
        Object limitBytes = rules.get(PolicyRuleKeyConstants.LIMIT_BYTES);
        return String.valueOf(toPositiveLong(limitBytes, PolicyRuleKeyConstants.LIMIT_BYTES));
    }

    private String normalizeTimeBlock(String newValue) {
        if (looksLikeJson(newValue)) {
            String normalized = newValue.trim();
            if (!HHMM_RANGE_PATTERN.matcher(normalized).matches()) {
                throw new IllegalArgumentException("TIME_BLOCK format must be HHMM-HHMM");
            }
            String[] tokens = normalized.split("-", -1);
            if (tokens.length != 2 || isValidHhmm(tokens[0]) || isValidHhmm(tokens[1])) {
                throw new IllegalArgumentException("Invalid TIME_BLOCK range");
            }
            return normalized;
        }

        Map<String, Object> rules = parseRulesJson(newValue);
        String start =
                toHhmm(rules.get(PolicyRuleKeyConstants.START), PolicyRuleKeyConstants.START);
        String end = toHhmm(rules.get(PolicyRuleKeyConstants.END), PolicyRuleKeyConstants.END);
        return start + "-" + end;
    }

    private String normalizeManualBlock(String newValue) {
        if (looksLikeJson(newValue)) {
            String normalized = newValue.trim();
            if ("1".equals(normalized)) {
                return "1";
            }
            if ("0".equals(normalized)) {
                return null;
            }
            throw new IllegalArgumentException("Invalid MANUAL_BLOCK value");
        }

        Map<String, Object> rules = parseRulesJson(newValue);
        Object reason = rules.get(PolicyRuleKeyConstants.REASON);
        if (reason == null || String.valueOf(reason).isBlank()) {
            throw new IllegalArgumentException("MANUAL_BLOCK requires reason");
        }
        return "1";
    }

    private String normalizeAppBlock(String newValue) {
        if (looksLikeJson(newValue)) {
            return normalizeCsvAppList(newValue);
        }

        Map<String, Object> rules = parseRulesJson(newValue);
        Object blockedApps = rules.get(PolicyRuleKeyConstants.BLOCKED_APPS);
        if (!(blockedApps instanceof List<?> appList)) {
            throw new IllegalArgumentException("blockedApps must be an array");
        }

        Set<String> apps =
                appList.stream()
                        .map(String::valueOf)
                        .map(String::trim)
                        .filter(appId -> !appId.isBlank())
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        if (apps.isEmpty()) {
            throw new IllegalArgumentException("blockedApps is empty");
        }
        return String.join(",", apps);
    }

    private String normalizeCsvAppList(String value) {
        Set<String> apps =
                List.of(value.split(",")).stream()
                        .map(String::trim)
                        .filter(appId -> !appId.isBlank())
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        if (apps.isEmpty()) {
            throw new IllegalArgumentException("blockedApps is empty");
        }
        return String.join(",", apps);
    }

    private long toPositiveLong(Object value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException("Missing " + fieldName);
        }
        try {
            long parsed = Long.parseLong(String.valueOf(value).trim());
            if (parsed <= 0) {
                throw new IllegalArgumentException(fieldName + " must be positive");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid number for " + fieldName, e);
        }
    }

    private String toHhmm(Object value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException("Missing " + fieldName);
        }
        String normalized = String.valueOf(value).replace(":", "").trim();
        if (!HHMM_PATTERN.matcher(normalized).matches() || isValidHhmm(normalized)) {
            throw new IllegalArgumentException("Invalid HHMM for " + fieldName);
        }
        return normalized;
    }

    private Map<String, Object> parseRulesJson(String json) {
        try {
            return objectMapper.readValue(json, new TypeReference<>() {});
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid JSON value", e);
        }
    }

    private boolean looksLikeJson(String value) {
        if (value == null) {
            return true;
        }
        String trimmed = value.trim();
        return !trimmed.startsWith("{") || !trimmed.endsWith("}");
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private boolean isValidHhmm(String hhmm) {
        int hh = Integer.parseInt(hhmm.substring(0, 2));
        int mm = Integer.parseInt(hhmm.substring(2, 4));
        return hh < 0 || hh > 23 || mm < 0 || mm > 59;
    }
}
