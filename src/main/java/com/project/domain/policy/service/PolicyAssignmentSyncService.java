package com.project.domain.policy.service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.domain.policy.entity.Policy;
import com.project.domain.policy.entity.PolicyAssignment;
import com.project.domain.policy.enums.PolicyType;
import com.project.domain.policy.repository.PolicyAssignmentRepository;
import com.project.domain.policy.repository.PolicyRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class PolicyAssignmentSyncService {
    private static final String POLICY_KEY_BLOCK_ACCESS = "BLOCK:ACCESS";
    private static final String POLICY_KEY_BLOCK_APP_PREFIX = "BLOCK:APP:";
    private static final String POLICY_KEY_BLOCK_TIME_START = "BLOCK:TIME:START";
    private static final String POLICY_KEY_BLOCK_TIME_END = "BLOCK:TIME:END";
    private static final String POLICY_KEY_LIMIT_PREFIX = "LIMIT:DATA:";
    private static final String POLICY_KEY_LIMIT_MONTHLY = "LIMIT:DATA:MONTHLY";

    // ERD rules 스키마 키
    private static final String RULE_KEY_LIMIT_BYTES = "limitBytes";
    private static final String RULE_KEY_START = "start";
    private static final String RULE_KEY_END = "end";
    private static final String RULE_KEY_REASON = "reason";
    private static final String RULE_KEY_BLOCKED_APPS = "blockedApps";

    // 정확 매칭 키: prefix가 없는 정책 키는 이 맵에 등록해서 policy type을 연결한다.
    private static final Map<String, PolicyType> EXACT_POLICY_KEY_TYPES =
            Map.of(
                    POLICY_KEY_BLOCK_ACCESS, PolicyType.MANUAL_BLOCK,
                    POLICY_KEY_BLOCK_TIME_START, PolicyType.TIME_BLOCK,
                    POLICY_KEY_BLOCK_TIME_END, PolicyType.TIME_BLOCK);

    // prefix 매칭 키: appId/period처럼 suffix가 붙는 동적 키를 관리한다.
    private static final Map<String, PolicyType> PREFIX_POLICY_KEY_TYPES =
            Map.of(
                    POLICY_KEY_BLOCK_APP_PREFIX,
                    PolicyType.APP_BLOCK,
                    POLICY_KEY_LIMIT_PREFIX,
                    PolicyType.MONTHLY_LIMIT);

    private final PolicyAssignmentRepository policyAssignmentRepository;
    private final PolicyRepository policyRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public void syncAssignment(
            Long familyId, Long targetCustomerId, String policyKey, String newValue) {
        PolicyType policyType = resolvePolicyType(policyKey).orElse(null);
        if (policyType == null) {
            log.error(
                    "Unsupported policy key. Skip assignment sync. familyId={}, customerId={},"
                            + " policyKey={}",
                    familyId,
                    targetCustomerId,
                    policyKey);
            return;
        }

        Optional<PolicyAssignment> assignmentOpt =
                findAssignmentByPolicyType(familyId, targetCustomerId, policyType);

        if (assignmentOpt.isEmpty()) {
            log.error(
                    "Policy assignment not found. Skip assignment sync. familyId={}, customerId={},"
                            + " policyType={}, policyKey={}",
                    familyId,
                    targetCustomerId,
                    policyType,
                    policyKey);
            return;
        }

        PolicyAssignment assignment = assignmentOpt.get();
        String updatedRules =
                mergeRulesByPolicyType(assignment.getRules(), policyType, policyKey, newValue);
        assignment.update(updatedRules, null, null);
        policyAssignmentRepository.save(assignment);
    }

    @Transactional(readOnly = true)
    public Map<String, String> loadEffectiveConstraints(Long familyId, Long customerId) {
        List<PolicyAssignment> assignments =
                policyAssignmentRepository.findEffectiveAssignments(familyId, customerId);
        if (assignments.isEmpty()) {
            return Map.of();
        }

        Map<Long, Policy> policyById =
                policyRepository
                        .findAllById(
                                assignments.stream()
                                        .map(PolicyAssignment::getPolicyId)
                                        .distinct()
                                        .toList())
                        .stream()
                        .filter(policy -> !policy.isDeleted() && policy.isActive())
                        .collect(Collectors.toMap(Policy::getId, policy -> policy));

        Map<String, String> constraints = new LinkedHashMap<>();

        // family-wide를 먼저 반영한 뒤 customer 전용으로 override
        assignments.stream()
                .filter(assignment -> assignment.getTargetCustomerId() == null)
                .forEach(
                        assignment ->
                                applyAssignmentConstraints(assignment, policyById, constraints));
        assignments.stream()
                .filter(assignment -> customerId.equals(assignment.getTargetCustomerId()))
                .forEach(
                        assignment ->
                                applyAssignmentConstraints(assignment, policyById, constraints));

        return constraints;
    }

    private Optional<PolicyAssignment> findAssignmentByPolicyType(
            Long familyId, Long targetCustomerId, PolicyType policyType) {
        if (targetCustomerId == null) {
            return policyAssignmentRepository.findFamilyPolicyByTypeForUpdate(familyId, policyType);
        }

        return policyAssignmentRepository.findByTargetAndTypeForUpdate(
                familyId, targetCustomerId, policyType);
    }

    private void applyAssignmentConstraints(
            PolicyAssignment assignment,
            Map<Long, Policy> policyById,
            Map<String, String> constraints) {
        if (!assignment.isActive()) {
            return;
        }

        Policy policy = policyById.get(assignment.getPolicyId());
        if (policy == null) {
            return;
        }

        Map<String, Object> rules = parseRulesToMap(assignment.getRules());
        long assignmentVersion = resolveAssignmentVersion(assignment);

        // 과거 데이터 호환을 위해 rules에 Redis 키가 직접 들어있는 경우도 허용
        rules.forEach(
                (key, value) -> {
                    if (value == null || !isConstraintKey(key)) {
                        return;
                    }
                    String stringValue = String.valueOf(value).trim();
                    if (!stringValue.isEmpty()) {
                        putConstraintWithVersion(constraints, key, stringValue, assignmentVersion);
                    }
                });

        // ERD 규격 rules JSON -> Redis constraints 변환
        if (policy.getPolicyType() == PolicyType.MONTHLY_LIMIT) {
            Long limitBytes = toPositiveLong(rules.get(RULE_KEY_LIMIT_BYTES));
            if (limitBytes != null) {
                putConstraintWithVersion(
                        constraints,
                        POLICY_KEY_LIMIT_MONTHLY,
                        String.valueOf(limitBytes),
                        assignmentVersion);
            }
            return;
        }

        if (policy.getPolicyType() == PolicyType.TIME_BLOCK) {
            String start = toHhmm(rules.get(RULE_KEY_START));
            String end = toHhmm(rules.get(RULE_KEY_END));
            if (start != null) {
                putConstraintWithVersion(
                        constraints, POLICY_KEY_BLOCK_TIME_START, start, assignmentVersion);
            }
            if (end != null) {
                putConstraintWithVersion(
                        constraints, POLICY_KEY_BLOCK_TIME_END, end, assignmentVersion);
            }
            return;
        }

        if (policy.getPolicyType() == PolicyType.MANUAL_BLOCK) {
            if (rules.get(RULE_KEY_REASON) != null) {
                putConstraintWithVersion(
                        constraints, POLICY_KEY_BLOCK_ACCESS, "1", assignmentVersion);
            }
            return;
        }

        if (policy.getPolicyType() == PolicyType.APP_BLOCK) {
            Object blockedAppsObj = rules.get(RULE_KEY_BLOCKED_APPS);
            if (blockedAppsObj instanceof List<?> blockedApps) {
                blockedApps.stream()
                        .map(String::valueOf)
                        .filter(appId -> !appId.isBlank())
                        .forEach(
                                appId ->
                                        putConstraintWithVersion(
                                                constraints,
                                                POLICY_KEY_BLOCK_APP_PREFIX + appId,
                                                "1",
                                                assignmentVersion));
            }
            return;
        }

    }

    private String mergeRulesByPolicyType(
            String rulesJson, PolicyType policyType, String policyKey, String newValue) {
        Map<String, Object> rules = parseRulesToMap(rulesJson);
        String normalizedValue = normalize(newValue);

        switch (policyType) {
            case MONTHLY_LIMIT -> mergeMonthlyLimitRule(rules, policyKey, normalizedValue);
            case TIME_BLOCK -> mergeTimeBlockRule(rules, policyKey, normalizedValue);
            case APP_BLOCK -> mergeAppBlockRule(rules, policyKey, normalizedValue);
            case MANUAL_BLOCK -> mergeManualBlockRule(rules, policyKey, normalizedValue);
            default -> {
                log.error(
                        "Unsupported policy type for rules merge. policyType={}, policyKey={}",
                        policyType,
                        policyKey);
                return rulesJson;
            }
        }

        try {
            return objectMapper.writeValueAsString(rules);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize policy assignment rules", e);
        }
    }

    private void mergeMonthlyLimitRule(
            Map<String, Object> rules, String policyKey, String newValue) {
        if (!policyKey.startsWith(POLICY_KEY_LIMIT_PREFIX)) {
            log.error("Unexpected key for MONTHLY_LIMIT. policyKey={}", policyKey);
            return;
        }

        if (newValue == null) {
            rules.remove(RULE_KEY_LIMIT_BYTES);
            return;
        }

        Long value = toPositiveLong(newValue);
        if (value != null) {
            rules.put(RULE_KEY_LIMIT_BYTES, value);
        }
    }

    private void mergeTimeBlockRule(Map<String, Object> rules, String policyKey, String newValue) {
        if (POLICY_KEY_BLOCK_TIME_START.equals(policyKey)) {
            if (newValue == null) {
                rules.remove(RULE_KEY_START);
            } else {
                rules.put(RULE_KEY_START, toTimeFormat(newValue));
            }
            return;
        }

        if (POLICY_KEY_BLOCK_TIME_END.equals(policyKey)) {
            if (newValue == null) {
                rules.remove(RULE_KEY_END);
            } else {
                rules.put(RULE_KEY_END, toTimeFormat(newValue));
            }
            return;
        }

        log.error("Unexpected key for TIME_BLOCK. policyKey={}", policyKey);
    }

    private void mergeAppBlockRule(Map<String, Object> rules, String policyKey, String newValue) {
        if (!policyKey.startsWith(POLICY_KEY_BLOCK_APP_PREFIX)) {
            log.error("Unexpected key for APP_BLOCK. policyKey={}", policyKey);
            return;
        }

        String appId = policyKey.substring(POLICY_KEY_BLOCK_APP_PREFIX.length());
        List<String> blockedApps = extractBlockedApps(rules.get(RULE_KEY_BLOCKED_APPS));

        if (newValue == null || "0".equals(newValue)) {
            blockedApps.remove(appId);
        } else {
            if (!blockedApps.contains(appId)) {
                blockedApps.add(appId);
            }
        }

        if (blockedApps.isEmpty()) {
            rules.remove(RULE_KEY_BLOCKED_APPS);
            return;
        }

        rules.put(RULE_KEY_BLOCKED_APPS, blockedApps);
    }

    private void mergeManualBlockRule(
            Map<String, Object> rules, String policyKey, String newValue) {
        if (!POLICY_KEY_BLOCK_ACCESS.equals(policyKey)) {
            log.error("Unexpected key for MANUAL_BLOCK. policyKey={}", policyKey);
            return;
        }

        if (newValue == null || "0".equals(newValue)) {
            rules.remove(RULE_KEY_REASON);
            return;
        }

        rules.put(RULE_KEY_REASON, "MANUAL");
    }

    private List<String> extractBlockedApps(Object blockedAppsObj) {
        if (!(blockedAppsObj instanceof List<?> rawList)) {
            return new ArrayList<>();
        }
        return rawList.stream()
                .map(String::valueOf)
                .distinct()
                .collect(Collectors.toCollection(ArrayList::new));
    }

    private Map<String, Object> parseRulesToMap(String rulesJson) {
        if (rulesJson == null || rulesJson.isBlank()) {
            return new LinkedHashMap<>();
        }
        try {
            return objectMapper.readValue(rulesJson, new TypeReference<>() {});
        } catch (Exception e) {
            log.warn("Failed to parse rules JSON. Fallback to empty rules. rules={}", rulesJson, e);
            return new LinkedHashMap<>();
        }
    }

    private Optional<PolicyType> resolvePolicyType(String policyKey) {
        if (policyKey == null || policyKey.isBlank()) {
            return Optional.empty();
        }

        PolicyType exactType = EXACT_POLICY_KEY_TYPES.get(policyKey);
        if (exactType != null) {
            return Optional.of(exactType);
        }

        for (Map.Entry<String, PolicyType> entry : PREFIX_POLICY_KEY_TYPES.entrySet()) {
            if (policyKey.startsWith(entry.getKey())) {
                return Optional.of(entry.getValue());
            }
        }

        return Optional.empty();
    }

    private boolean isConstraintKey(String key) {
        if (EXACT_POLICY_KEY_TYPES.containsKey(key)) {
            return true;
        }

        for (String prefix : PREFIX_POLICY_KEY_TYPES.keySet()) {
            if (key.startsWith(prefix)) {
                return true;
            }
        }

        return false;
    }

    private String normalize(String newValue) {
        if (newValue == null || newValue.isBlank()) {
            return null;
        }
        return newValue.trim();
    }

    private void putConstraintWithVersion(
            Map<String, String> constraints, String policyKey, String value, long version) {
        constraints.put(policyKey, value);
        constraints.put(buildVersionField(policyKey), String.valueOf(version));
    }

    private String buildVersionField(String policyKey) {
        return "ver:" + policyKey;
    }

    private long resolveAssignmentVersion(PolicyAssignment assignment) {
        if (assignment.getUpdatedAt() != null) {
            return assignment
                    .getUpdatedAt()
                    .atZone(java.time.ZoneOffset.UTC)
                    .toInstant()
                    .toEpochMilli();
        }
        if (assignment.getCreatedAt() != null) {
            return assignment
                    .getCreatedAt()
                    .atZone(java.time.ZoneOffset.UTC)
                    .toInstant()
                    .toEpochMilli();
        }
        return System.currentTimeMillis();
    }

    private Long toPositiveLong(Object value) {
        if (value == null) {
            return null;
        }
        try {
            long parsed = Long.parseLong(String.valueOf(value));
            return parsed > 0 ? parsed : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private String toTimeFormat(String hhmm) {
        if (hhmm == null) {
            return null;
        }
        String normalized = hhmm.trim();
        if (normalized.length() != 4) {
            return normalized;
        }
        return normalized.substring(0, 2) + ":" + normalized.substring(2, 4);
    }

    private String toHhmm(Object timeValue) {
        if (timeValue == null) {
            return null;
        }
        String normalized = String.valueOf(timeValue).replace(":", "").trim();
        if (normalized.length() != 4) {
            return null;
        }
        return normalized;
    }
}
