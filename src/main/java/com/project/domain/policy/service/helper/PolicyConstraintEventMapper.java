package com.project.domain.policy.service.helper;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.domain.policy.constant.PolicyConstraintKeyConstants;
import com.project.domain.policy.constant.PolicyRuleKeyConstants;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class PolicyConstraintEventMapper {
    private static final Pattern HHMM_PATTERN = Pattern.compile("^\\d{4}$");

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
        JsonNode rules = parseRulesJson(newValue);
        JsonNode limitBytesNode = rules.get(PolicyRuleKeyConstants.LIMIT_BYTES);
        return String.valueOf(toPositiveLong(limitBytesNode, PolicyRuleKeyConstants.LIMIT_BYTES));
    }

    private String normalizeTimeBlock(String newValue) {
        JsonNode rules = parseRulesJson(newValue);
        String start =
                toHhmm(rules.get(PolicyRuleKeyConstants.START), PolicyRuleKeyConstants.START);
        String end = toHhmm(rules.get(PolicyRuleKeyConstants.END), PolicyRuleKeyConstants.END);
        return start + "-" + end;
    }

    private String normalizeManualBlock(String newValue) {
        JsonNode rules = parseRulesJson(newValue);
        JsonNode reasonNode = rules.get(PolicyRuleKeyConstants.REASON);
        if (reasonNode == null || reasonNode.isNull() || reasonNode.asText().isBlank()) {
            throw new IllegalArgumentException("MANUAL_BLOCK requires reason");
        }
        return "1";
    }

    private String normalizeAppBlock(String newValue) {
        JsonNode rules = parseRulesJson(newValue);
        JsonNode blockedAppsNode = rules.get(PolicyRuleKeyConstants.BLOCKED_APPS);
        if (blockedAppsNode == null || !blockedAppsNode.isArray()) {
            throw new IllegalArgumentException("blockedApps must be an array");
        }

        Set<String> apps =
                toList(blockedAppsNode).stream()
                        .map(JsonNode::asText)
                        .map(String::trim)
                        .filter(appId -> !appId.isBlank())
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        if (apps.isEmpty()) {
            throw new IllegalArgumentException("blockedApps is empty");
        }
        return String.join(",", apps);
    }

    private List<JsonNode> toList(JsonNode arrayNode) {
        java.util.ArrayList<JsonNode> result = new java.util.ArrayList<>();
        arrayNode.forEach(result::add);
        return result;
    }

    private long toPositiveLong(JsonNode value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException("Missing " + fieldName);
        }
        try {
            if (!value.isNumber() && !value.isTextual()) {
                throw new IllegalArgumentException("Invalid number type for " + fieldName);
            }
            long parsed = Long.parseLong(value.asText().trim());
            if (parsed <= 0) {
                throw new IllegalArgumentException(fieldName + " must be positive");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid number for " + fieldName, e);
        }
    }

    private String toHhmm(JsonNode value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException("Missing " + fieldName);
        }
        if (!value.isTextual()) {
            throw new IllegalArgumentException("Invalid HHMM type for " + fieldName);
        }
        String normalized = value.asText().replace(":", "").trim();
        if (!HHMM_PATTERN.matcher(normalized).matches() || isValidHhmm(normalized)) {
            throw new IllegalArgumentException("Invalid HHMM for " + fieldName);
        }
        return normalized;
    }

    private JsonNode parseRulesJson(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);
            if (root == null || !root.isObject()) {
                throw new IllegalArgumentException("JSON value must be an object");
            }
            return root;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid JSON value", e);
        }
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
