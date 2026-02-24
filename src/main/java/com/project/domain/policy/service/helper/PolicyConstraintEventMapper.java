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
        // 삭제 이벤트(빈 값)는 Redis에서 HDEL 대상이 되도록 null 반환
        if (isBlank(newValue)) {
            return null;
        }
        // policyKey별 rules JSON 스키마를 Redis 저장 포맷으로 정규화
        return switch (policyKey) {
            case PolicyConstraintKeyConstants.LIMIT_DATA_MONTHLY -> normalizeMonthlyLimit(newValue);
            case PolicyConstraintKeyConstants.BLOCK_TIME -> normalizeTimeBlock(newValue);
            case PolicyConstraintKeyConstants.BLOCK_ACCESS -> normalizeManualBlock(newValue);
            case PolicyConstraintKeyConstants.BLOCK_APP -> normalizeAppBlock(newValue);
            default -> throw new IllegalArgumentException("Unsupported policy key: " + policyKey);
        };
    }

    private String normalizeMonthlyLimit(String newValue) {
        // {"limitBytes": 123} -> "123"
        JsonNode rules = parseRulesJson(newValue);
        JsonNode limitBytesNode = rules.get(PolicyRuleKeyConstants.LIMIT_BYTES);
        return String.valueOf(toPositiveLong(limitBytesNode, PolicyRuleKeyConstants.LIMIT_BYTES));
    }

    private String normalizeTimeBlock(String newValue) {
        // {"start":"22:00","end":"07:00"} -> "2200-0700"
        JsonNode rules = parseRulesJson(newValue);
        String start =
                toHhmm(rules.get(PolicyRuleKeyConstants.START), PolicyRuleKeyConstants.START);
        String end = toHhmm(rules.get(PolicyRuleKeyConstants.END), PolicyRuleKeyConstants.END);
        return start + "-" + end;
    }

    private String normalizeManualBlock(String newValue) {
        // reason 존재 여부로 수동 차단 활성화 판단 -> Redis 값은 "1"
        JsonNode rules = parseRulesJson(newValue);
        JsonNode reasonNode = rules.get(PolicyRuleKeyConstants.REASON);
        if (reasonNode == null || reasonNode.isNull() || reasonNode.asText().isBlank()) {
            throw new IllegalArgumentException("MANUAL_BLOCK requires reason");
        }
        return "1";
    }

    private String normalizeAppBlock(String newValue) {
        // {"blockedApps":[...]} -> "app1,app2" (로그/호환용 문자열)
        return String.join(",", normalizeAppBlockValueAsSet(newValue));
    }

    public Set<String> normalizeAppBlockValueAsSet(String newValue) {
        // BLOCK:APP 동기화용으로 앱 ID 집합을 직접 반환
        if (isBlank(newValue)) {
            return Set.of();
        }
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
        return apps;
    }

    private List<JsonNode> toList(JsonNode arrayNode) {
        java.util.ArrayList<JsonNode> result = new java.util.ArrayList<>();
        arrayNode.forEach(result::add);
        return result;
    }

    private long toPositiveLong(JsonNode value, String fieldName) {
        // 숫자/문자 숫자 모두 허용하되 양수만 유효
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
        // HH:mm 또는 HHmm 입력을 HHmm으로 정규화 후 범위 검증
        if (value == null) {
            throw new IllegalArgumentException("Missing " + fieldName);
        }
        if (!value.isTextual()) {
            throw new IllegalArgumentException("Invalid HHMM type for " + fieldName);
        }
        String normalized = value.asText().replace(":", "").trim();
        if (!HHMM_PATTERN.matcher(normalized).matches() || !isValidHhmm(normalized)) {
            throw new IllegalArgumentException("Invalid HHMM for " + fieldName);
        }
        return normalized;
    }

    private JsonNode parseRulesJson(String json) {
        // policy newValue는 JSON object여야 한다.
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
        // HHmm 값의 시/분 범위가 유효한지 확인
        int hh = Integer.parseInt(hhmm.substring(0, 2));
        int mm = Integer.parseInt(hhmm.substring(2, 4));
        return hh >= 0 && hh <= 23 && mm >= 0 && mm <= 59;
    }
}
