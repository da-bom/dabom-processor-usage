package com.project.domain.policy.service.helper;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.domain.policy.constant.PolicyRuleKeyConstants;
import com.project.domain.policy.entity.Policy;
import com.project.domain.policy.entity.PolicyAssignment;
import com.project.domain.policy.enums.PolicyType;
import com.project.domain.policy.infra.cache.dto.PolicyConstraintRedisHash;
import com.project.domain.policy.repository.PolicyAssignmentRepository;
import com.project.domain.policy.repository.PolicyRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class PolicyAssignmentSyncHelper {
    private final PolicyAssignmentRepository policyAssignmentRepository;
    private final PolicyRepository policyRepository;
    private final ObjectMapper objectMapper;

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

        PolicyConstraintRedisHash constraints = PolicyConstraintRedisHash.create();

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

        return constraints.toMap();
    }

    // 단일 assignment를 Redis constraints 키-값 집합으로 반영
    private void applyAssignmentConstraints(
            PolicyAssignment assignment,
            Map<Long, Policy> policyById,
            PolicyConstraintRedisHash constraints) {
        // 비활성 assignment 또는 policy 템플릿 미존재 건은 제약 계산 대상에서 제외
        if (!isApplicableAssignment(assignment, policyById)) {
            return;
        }

        Policy policy = policyById.get(assignment.getPolicyId());
        Map<String, Object> rules = parseRulesToMap(assignment.getRules());
        long assignmentVersion = resolveAssignmentVersion(assignment);

        // ERD 표준: policyType + rules JSON 스키마를 Redis constraints로 변환
        applyErdRulesByPolicyType(policy.getPolicyType(), rules, constraints, assignmentVersion);
    }

    // 제약 계산 대상인지(활성 + 유효 policy 존재) 판별
    private boolean isApplicableAssignment(
            PolicyAssignment assignment, Map<Long, Policy> policyById) {
        return assignment.isActive() && policyById.get(assignment.getPolicyId()) != null;
    }

    // 정책 타입별 ERD rules 변환 함수를 호출
    private void applyErdRulesByPolicyType(
            PolicyType policyType,
            Map<String, Object> rules,
            PolicyConstraintRedisHash constraints,
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
            Map<String, Object> rules,
            PolicyConstraintRedisHash constraints,
            long assignmentVersion) {
        // limitBytes(ERD) -> LIMIT:DATA:MONTHLY(Redis)
        Long limitBytes = toPositiveLong(rules.get(PolicyRuleKeyConstants.LIMIT_BYTES));
        if (limitBytes == null) {
            return;
        }
        constraints.putMonthlyLimit(limitBytes, assignmentVersion);
    }

    // 시간대 차단 정책의 rules를 시작/종료 제약으로 변환
    private void applyTimeBlockConstraint(
            Map<String, Object> rules,
            PolicyConstraintRedisHash constraints,
            long assignmentVersion) {
        // start/end(ERD, HH:mm) -> BLOCK:TIME:START/END(Redis, HHmm)
        String start = toHhmm(rules.get(PolicyRuleKeyConstants.START));
        String end = toHhmm(rules.get(PolicyRuleKeyConstants.END));

        if (start != null) {
            constraints.putTimeBlockStart(start, assignmentVersion);
        }
        if (end != null) {
            constraints.putTimeBlockEnd(end, assignmentVersion);
        }
    }

    // 수동 차단 정책의 rules를 접근 차단 제약으로 변환
    private void applyManualBlockConstraint(
            Map<String, Object> rules,
            PolicyConstraintRedisHash constraints,
            long assignmentVersion) {
        // reason 값이 존재하면 접근 차단 활성화로 간주
        if (rules.get(PolicyRuleKeyConstants.REASON) == null) {
            return;
        }
        constraints.putManualBlock(assignmentVersion);
    }

    // 앱 차단 배열을 개별 BLOCK:APP:{appId} 제약들로 확장
    private void applyAppBlockConstraint(
            Map<String, Object> rules,
            PolicyConstraintRedisHash constraints,
            long assignmentVersion) {
        // blockedApps 배열의 각 appId를 BLOCK:APP:{appId}=1 제약으로 반영
        Object blockedAppsObj = rules.get(PolicyRuleKeyConstants.BLOCKED_APPS);
        if (!(blockedAppsObj instanceof List<?> blockedApps)) {
            return;
        }

        blockedApps.stream()
                .map(String::valueOf)
                .filter(appId -> !appId.isBlank())
                .forEach(appId -> constraints.putBlockedApp(appId, assignmentVersion));
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
