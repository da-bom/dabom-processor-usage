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

    // policy-updated 이벤트를 DB policy_assignment.rules에 반영
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

    // 특정 고객의 최종 유효 정책을 계산해 Redis constraints 형태로 변환
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

    // family-wide/고객별 케이스에 따라 정책 할당 레코드를 잠금 조회
    private Optional<PolicyAssignment> findAssignmentByPolicyType(
            Long familyId, Long targetCustomerId, PolicyType policyType) {
        if (targetCustomerId == null) {
            return policyAssignmentRepository.findFamilyPolicyByTypeForUpdate(familyId, policyType);
        }

        return policyAssignmentRepository.findByTargetAndTypeForUpdate(
                familyId, targetCustomerId, policyType);
    }

    // 단일 assignment를 Redis constraints 키-값 집합으로 반영
    private void applyAssignmentConstraints(
            PolicyAssignment assignment,
            Map<Long, Policy> policyById,
            Map<String, String> constraints) {
        // 비활성 assignment 또는 policy 템플릿 미존재 건은 제약 계산 대상에서 제외
        if (!isApplicableAssignment(assignment, policyById)) {
            return;
        }

        Policy policy = policyById.get(assignment.getPolicyId());
        Map<String, Object> rules = parseRulesToMap(assignment.getRules());
        long assignmentVersion = resolveAssignmentVersion(assignment);

        // 1) 레거시 호환: rules 안에 Redis 정책 키가 직접 저장된 형태를 먼저 반영
        applyLegacyDirectConstraintRules(rules, constraints, assignmentVersion);
        // 2) ERD 표준: policyType + rules JSON 스키마를 Redis constraints로 변환
        applyErdRulesByPolicyType(policy.getPolicyType(), rules, constraints, assignmentVersion);
    }

    // 제약 계산 대상인지(활성 + 유효 policy 존재) 판별
    private boolean isApplicableAssignment(
            PolicyAssignment assignment, Map<Long, Policy> policyById) {
        return assignment.isActive() && policyById.get(assignment.getPolicyId()) != null;
    }

    // 레거시 규격(rules에 Redis 키 직접 저장) 데이터를 constraints로 옮김
    private void applyLegacyDirectConstraintRules(
            Map<String, Object> rules, Map<String, String> constraints, long assignmentVersion) {
        // 과거 데이터 호환을 위해 rules에 Redis 키가 직접 들어있는 경우도 허용
        rules.forEach(
                (key, value) -> {
                    if (value == null || !isConstraintKey(key)) {
                        return;
                    }
                    String stringValue = String.valueOf(value).trim();
                    if (stringValue.isEmpty()) {
                        return;
                    }
                    putConstraintWithVersion(constraints, key, stringValue, assignmentVersion);
                });
    }

    // 정책 타입별 ERD rules 변환 함수를 호출
    private void applyErdRulesByPolicyType(
            PolicyType policyType,
            Map<String, Object> rules,
            Map<String, String> constraints,
            long assignmentVersion) {
        // policy type마다 rules JSON 스키마가 다르므로 전용 변환기로 분기
        switch (policyType) {
            case MONTHLY_LIMIT ->
                    applyMonthlyLimitConstraint(rules, constraints, assignmentVersion);
            case TIME_BLOCK -> applyTimeBlockConstraint(rules, constraints, assignmentVersion);
            case MANUAL_BLOCK -> applyManualBlockConstraint(rules, constraints, assignmentVersion);
            case APP_BLOCK -> applyAppBlockConstraint(rules, constraints, assignmentVersion);
            default ->
                    log.warn(
                            "Unsupported policy type for ERD rules conversion. policyType={}",
                            policyType);
        }
    }

    // 월 제한 정책의 rules를 LIMIT:DATA:MONTHLY 제약으로 변환
    private void applyMonthlyLimitConstraint(
            Map<String, Object> rules, Map<String, String> constraints, long assignmentVersion) {
        // limitBytes(ERD) -> LIMIT:DATA:MONTHLY(Redis)
        Long limitBytes = toPositiveLong(rules.get(RULE_KEY_LIMIT_BYTES));
        if (limitBytes == null) {
            return;
        }
        putConstraintWithVersion(
                constraints,
                POLICY_KEY_LIMIT_MONTHLY,
                String.valueOf(limitBytes),
                assignmentVersion);
    }

    // 시간대 차단 정책의 rules를 시작/종료 제약으로 변환
    private void applyTimeBlockConstraint(
            Map<String, Object> rules, Map<String, String> constraints, long assignmentVersion) {
        // start/end(ERD, HH:mm) -> BLOCK:TIME:START/END(Redis, HHmm)
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
    }

    // 수동 차단 정책의 rules를 접근 차단 제약으로 변환
    private void applyManualBlockConstraint(
            Map<String, Object> rules, Map<String, String> constraints, long assignmentVersion) {
        // reason 값이 존재하면 접근 차단 활성화로 간주
        if (rules.get(RULE_KEY_REASON) == null) {
            return;
        }
        putConstraintWithVersion(constraints, POLICY_KEY_BLOCK_ACCESS, "1", assignmentVersion);
    }

    // 앱 차단 배열을 개별 BLOCK:APP:{appId} 제약들로 확장
    private void applyAppBlockConstraint(
            Map<String, Object> rules, Map<String, String> constraints, long assignmentVersion) {
        // blockedApps 배열의 각 appId를 BLOCK:APP:{appId}=1 제약으로 반영
        Object blockedAppsObj = rules.get(RULE_KEY_BLOCKED_APPS);
        if (!(blockedAppsObj instanceof List<?> blockedApps)) {
            return;
        }

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

    // policyType에 맞춰 rules JSON을 업데이트한 뒤 다시 직렬화
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

    // 월 제한 정책 키를 검증하고 limitBytes 값을 추가/삭제
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

    // 시간대 정책 키(START/END)에 따라 start/end 값을 갱신
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

    // 앱 차단 정책 키에서 appId를 추출해 blockedApps 배열을 갱신
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

    // 수동 차단 정책 값을 기준으로 reason 필드를 추가/삭제
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

    // blockedApps 원본 객체를 중복 없는 문자열 리스트로 정규화
    private List<String> extractBlockedApps(Object blockedAppsObj) {
        if (!(blockedAppsObj instanceof List<?> rawList)) {
            return new ArrayList<>();
        }
        return rawList.stream()
                .map(String::valueOf)
                .distinct()
                .collect(Collectors.toCollection(ArrayList::new));
    }

    // rules JSON 문자열을 Map으로 파싱하고 실패 시 빈 맵으로 대체
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

    // 정책 키를 exact/prefix 매핑으로 해석해 PolicyType을 결정
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

    // 입력 키가 Redis constraints 정책 키인지 여부를 판별
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

    // 이벤트의 newValue를 trim하고 빈 값은 null로 통일
    private String normalize(String newValue) {
        if (newValue == null || newValue.isBlank()) {
            return null;
        }
        return newValue.trim();
    }

    // 정책 값과 버전 필드(ver:policyKey)를 constraints 맵에 함께 기록
    private void putConstraintWithVersion(
            Map<String, String> constraints, String policyKey, String value, long version) {
        constraints.put(policyKey, value);
        constraints.put(buildVersionField(policyKey), String.valueOf(version));
    }

    // Lua stale 방지를 위한 버전 필드 키를 생성
    private String buildVersionField(String policyKey) {
        return "ver:" + policyKey;
    }

    // assignment의 시간 정보를 epoch millis 버전으로 변환
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

    // 양수 long 값만 허용하고 나머지는 null로 처리
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

    // HHmm 문자열을 HH:mm 형식으로 변환
    private String toTimeFormat(String hhmm) {
        if (hhmm == null) {
            return null;
        }
        String normalized = hhmm.trim();
        if (normalized.length() != 4) {
            return null;
        }
        return normalized.substring(0, 2) + ":" + normalized.substring(2, 4);
    }

    // HH:mm 또는 HHmm 입력을 Lua 규격(HHmm)으로 정규화
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
