package com.project.domain.policy.service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.domain.family.entity.FamilyMember;
import com.project.domain.family.repository.FamilyMemberRepository;
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
    public static final String POLICY_CONSTRAINT_CACHE = "policyConstraintCache";

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
    private final FamilyMemberRepository familyMemberRepository;
    private final CacheManager cacheManager;
    private final ObjectMapper objectMapper;

    @Transactional
    public void syncAssignment(
            Long familyId, Long targetCustomerId, String policyKey, String newValue) {
        // 1) policyType 매핑 기반으로 우선 탐색
        Optional<PolicyAssignment> assignmentOpt =
                findAssignmentByPolicyType(familyId, targetCustomerId, policyKey);

        // 2) 못 찾으면 rules 내부에 해당 키가 있는 assignment를 fallback 탐색
        if (assignmentOpt.isEmpty()) {
            assignmentOpt = findAssignmentByConstraintKey(familyId, targetCustomerId, policyKey);
        }

        if (assignmentOpt.isEmpty()) {
            log.warn(
                    "Policy assignment not found. familyId={}, customerId={}, policyKey={}",
                    familyId,
                    targetCustomerId,
                    policyKey);
            return;
        }

        // rules JSON은 Redis 정책 키 그대로 저장/갱신한다
        // newValue가 blank/null이면 해당 정책 키를 rules에서 제거한다
        PolicyAssignment assignment = assignmentOpt.get();
        String updatedRules = mergeRules(assignment.getRules(), policyKey, newValue);
        assignment.update(updatedRules, null, null);
        policyAssignmentRepository.save(assignment);

        // 캐시는 영향받는 키만 무효화한다
        evictConstraintCacheByScope(familyId, targetCustomerId);
    }

    @Cacheable(
            cacheNames = POLICY_CONSTRAINT_CACHE,
            key = "#familyId + ':' + #customerId",
            sync = true)
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

    // targetCustomerId가 있으면 해당 customer 키만, 없으면 family 전체 customer 키를 무효화한다.
    public void evictConstraintCacheByScope(Long familyId, Long targetCustomerId) {
        if (targetCustomerId != null) {
            evictConstraintCacheKey(familyId, targetCustomerId);
            return;
        }

        familyMemberRepository.findAllByFamilyIdAndDeletedAtIsNull(familyId).stream()
                .map(FamilyMember::getCustomerId)
                .forEach(customerId -> evictConstraintCacheKey(familyId, customerId));
    }

    private void evictConstraintCacheKey(Long familyId, Long customerId) {
        Cache cache = cacheManager.getCache(POLICY_CONSTRAINT_CACHE);
        if (cache == null) {
            return;
        }
        cache.evict(buildCacheKey(familyId, customerId));
    }

    private Optional<PolicyAssignment> findAssignmentByPolicyType(
            Long familyId, Long targetCustomerId, String policyKey) {
        // policyKey -> policyType 변환에 실패하면 type 기반 조회를 건너뛴다
        PolicyType policyType = resolvePolicyType(policyKey).orElse(null);
        if (policyType == null) {
            return Optional.empty();
        }

        // targetCustomerId가 null이면 가족 전체 정책 조회
        if (targetCustomerId == null) {
            return policyAssignmentRepository.findFamilyPolicyByType(familyId, policyType);
        }

        return policyAssignmentRepository.findByTargetAndType(
                familyId, targetCustomerId, policyType);
    }

    private Optional<PolicyAssignment> findAssignmentByConstraintKey(
            Long familyId, Long targetCustomerId, String policyKey) {
        // type 기반 조회 실패 시, rules 내부에 실제 키가 들어있는 assignment를 fallback으로 찾는다
        List<PolicyAssignment> candidates =
                targetCustomerId == null
                        ? policyAssignmentRepository.findAllByFamilyId(familyId).stream()
                                .filter(assignment -> assignment.getTargetCustomerId() == null)
                                .toList()
                        : policyAssignmentRepository.findEffectiveAssignments(
                                familyId, targetCustomerId);

        return candidates.stream()
                .filter(PolicyAssignment::isActive)
                .filter(assignment -> containsPolicyKey(assignment.getRules(), policyKey))
                .findFirst();
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
        rules.forEach(
                (key, value) -> {
                    // 문서에 정의된 정책 키만 constraints로 반영한다.
                    if (value == null || !isConstraintKey(key)) {
                        return;
                    }

                    String stringValue = String.valueOf(value).trim();
                    if (!stringValue.isEmpty()) {
                        constraints.put(key, stringValue);
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

    private boolean containsPolicyKey(String rulesJson, String policyKey) {
        return parseRulesToMap(rulesJson).containsKey(policyKey);
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

    private String buildCacheKey(Long familyId, Long customerId) {
        return familyId + ":" + customerId;
    }
}
