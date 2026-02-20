package com.project.domain.policy.service;

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
    // 정확 매칭 키:
    // prefix가 없는 정책 키는 이 맵에 등록해서 policy type을 연결한다.
    private static final Map<String, PolicyType> EXACT_POLICY_KEY_TYPES =
            Map.of(
                    "BLOCK:ACCESS", PolicyType.MANUAL_BLOCK,
                    "BLOCK:TIME:START", PolicyType.TIME_BLOCK,
                    "BLOCK:TIME:END", PolicyType.TIME_BLOCK);

    // prefix 매칭 키:
    // appId/period처럼 suffix가 붙는 동적 키를 관리한다.
    private static final Map<String, PolicyType> PREFIX_POLICY_KEY_TYPES =
            Map.of("BLOCK:APP:", PolicyType.APP_BLOCK, "LIMIT:DATA:", PolicyType.MONTHLY_LIMIT);

    private final PolicyAssignmentRepository policyAssignmentRepository;
    private final PolicyRepository policyRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public void syncAssignment(
            Long familyId, Long targetCustomerId, String policyKey, String newValue) {
        // policyKey -> policyType 매핑이 실패하면 데이터 계약 불일치로 보고 에러 로그 후 종료
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

        // rules JSON은 Redis 정책 키 그대로 저장/갱신한다
        // newValue가 blank/null이면 해당 정책 키를 rules에서 제거한다
        PolicyAssignment assignment = assignmentOpt.get();
        String updatedRules = mergeRules(assignment.getRules(), policyKey, newValue);
        assignment.update(updatedRules, null, null);
        policyAssignmentRepository.save(assignment);
    }

    @Transactional(readOnly = true)
    public Map<String, String> loadEffectiveConstraints(Long familyId, Long customerId) {
        // customer에 적용 가능한 assignment는 family-wide + customer-specific 두 종류다.
        List<PolicyAssignment> assignments =
                policyAssignmentRepository.findEffectiveAssignments(familyId, customerId);
        if (assignments.isEmpty()) {
            return Map.of();
        }

        // soft-delete/비활성 policy는 제외하고 policy metadata를 로딩한다.
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
        // (동일 키가 있으면 customer 전용 값이 최종 우선순위를 가진다)
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
        // targetCustomerId가 null이면 가족 전체 정책 조회
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
        // 비활성 assignment는 Redis effective constraints에 포함하지 않는다.
        if (!assignment.isActive()) {
            return;
        }

        // 연결된 policy가 없거나 비활성이면 안전하게 skip한다.
        Policy policy = policyById.get(assignment.getPolicyId());
        if (policy == null) {
            return;
        }

        Map<String, Object> rules = parseRulesToMap(assignment.getRules());
        long assignmentVersion = resolveAssignmentVersion(assignment);
        rules.forEach(
                (key, value) -> {
                    // 문서에 정의된 정책 키만 constraints로 반영한다.
                    if (value == null || !isConstraintKey(key)) {
                        return;
                    }

                    String stringValue = String.valueOf(value).trim();
                    if (!stringValue.isEmpty()) {
                        constraints.put(key, stringValue);
                        // warmup 시에도 Lua stale 가드가 동작하도록 버전 필드를 함께 채운다.
                        constraints.put(buildVersionField(key), String.valueOf(assignmentVersion));
                    }
                });
    }

    private String mergeRules(String rulesJson, String policyKey, String newValue) {
        // 기존 rules JSON을 map으로 파싱 후 단일 키를 upsert/remove한다.
        Map<String, Object> rules = parseRulesToMap(rulesJson);
        String normalizedValue = normalize(newValue);

        if (normalizedValue == null) {
            rules.remove(policyKey);
        } else {
            rules.put(policyKey, normalizedValue);
        }

        try {
            return objectMapper.writeValueAsString(rules);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize policy assignment rules", e);
        }
    }

    private Map<String, Object> parseRulesToMap(String rulesJson) {
        if (rulesJson == null || rulesJson.isBlank()) {
            // 신규 assignment 또는 빈 rules를 안전하게 처리하기 위해 빈 map 사용
            return new LinkedHashMap<>();
        }
        try {
            return objectMapper.readValue(rulesJson, new TypeReference<>() {});
        } catch (Exception e) {
            // 깨진 JSON이 들어와도 전체 처리 중단 대신 빈 map으로 fallback한다.
            log.warn("Failed to parse rules JSON. Fallback to empty rules. rules={}", rulesJson, e);
            return new LinkedHashMap<>();
        }
    }

    private Optional<PolicyType> resolvePolicyType(String policyKey) {
        if (policyKey == null || policyKey.isBlank()) {
            return Optional.empty();
        }

        // 1) exact key lookup
        PolicyType exactType = EXACT_POLICY_KEY_TYPES.get(policyKey);
        if (exactType != null) {
            return Optional.of(exactType);
        }

        // 2) prefix key lookup
        for (Map.Entry<String, PolicyType> entry : PREFIX_POLICY_KEY_TYPES.entrySet()) {
            if (policyKey.startsWith(entry.getKey())) {
                return Optional.of(entry.getValue());
            }
        }

        return Optional.empty();
    }

    private boolean isConstraintKey(String key) {
        // exact key면 즉시 true
        if (EXACT_POLICY_KEY_TYPES.containsKey(key)) {
            return true;
        }

        // prefix key면 true
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
}
