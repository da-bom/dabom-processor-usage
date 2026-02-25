package com.project.domain.policy.service;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.project.global.common.TimeConstants;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import com.project.domain.family.repository.FamilyMemberRepository;
import com.project.domain.policy.constant.PolicyConstraintKeyConstants;
import com.project.domain.policy.service.helper.PolicyConstraintEventMapper;
import com.project.domain.policy.service.helper.PolicyConstraintWarmupHelper;
import com.project.domain.policy.service.helper.PolicyEventValidator;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.policy.PolicyUpdatedPayload;
import com.project.global.exception.ApplicationException;
import com.project.global.exception.code.PolicyErrorCode;
import com.project.global.util.LogSanitizer;
import com.project.global.util.RedisKeyGenerator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class PolicyConstraintSyncServiceImpl implements PolicyConstraintSyncService {
    private static final String VALUE_LOG_SUFFIX = ", value={}";
    private static final String LUA_RESULT_APPLIED = "APPLIED";

    private final RedisTemplate<String, String> familyStringRedisTemplate;
    private final RedisScript<List<String>> policyConstraintUpdateScript;
    private final RedisKeyGenerator redisKeyGenerator;
    private final FamilyMemberRepository familyMemberRepository;
    private final PolicyEventValidator policyEventValidator;
    private final PolicyConstraintEventMapper policyConstraintEventMapper;
    private final PolicyConstraintWarmupHelper policyConstraintWarmupHelper;
    private final LogSanitizer logSanitizer;

    @Value("${app.kafka.dedup.policy-ttl-seconds}")
    private long dedupTtlSeconds;

    // policy-updated 이벤트의 진입점:
    // 1) payload 검증 -> 2) Redis warmup -> 3) Lua 적용
    @Override
    public void sync(EventEnvelope<PolicyUpdatedPayload> envelope, String recordKey) {
        PolicyUpdatedPayload payload = envelope.payload();
        String eventId = envelope.eventId();

        // payload/eventId/필수값 검증
        if (!policyEventValidator.isValidPayload(payload, eventId, recordKey)) {
            return;
        }

        // 이벤트 payload에서 정책 키/대상/버전을 추출
        String policyKey = payload.policyKey();
        Long targetCustomerId = payload.targetCustomerId();
        // timestamp를 버전으로 사용해 역순 이벤트에서도 최신값만 반영
        long eventVersion = resolveEventVersion(envelope);

        // 삭제/갱신 모두 policyKey whitelist 검증
        if (!policyEventValidator.isAllowedPolicyKey(policyKey)) {
            log.warn(
                    "Invalid policy key. eventId={}, familyId={}, customerId={}, field={}",
                    logSanitizer.sanitize(eventId),
                    payload.familyId(),
                    targetCustomerId,
                    logSanitizer.sanitize(policyKey));
            return;
        }

        // 정책 타입별로 Redis 저장 형식에 맞는 값으로 정규화
        NormalizedPolicyValue normalizedPolicyValue =
                resolveNormalizedPolicyValue(payload, eventId, targetCustomerId);
        if (normalizedPolicyValue == null) {
            return;
        }

        // familyId/targetCustomerId가 모두 없으면 전체 활성 구성원에 대해 정책을 반영
        if (payload.familyId() == null && targetCustomerId == null) {
            processGlobalPolicyUpdate(eventId, policyKey, eventVersion, normalizedPolicyValue);
            return;
        }

        // targetCustomerId가 있으면 해당 customer만 반영
        if (targetCustomerId != null) {
            // family-customer 소속 관계 검증
            if (!isValidFamilyMember(payload.familyId(), targetCustomerId)) {
                log.warn(
                        "Skip policy update due to invalid family-customer relation."
                                + " eventId={}, familyId={}, customerId={}, field={}",
                        logSanitizer.sanitize(eventId),
                        payload.familyId(),
                        targetCustomerId,
                        logSanitizer.sanitize(policyKey));
                return;
            }

            // 단일 고객 정책 업데이트(워밍업 + Redis 반영)
            processCustomerPolicyUpdate(
                    eventId,
                    payload.familyId(),
                    targetCustomerId,
                    policyKey,
                    eventVersion,
                    normalizedPolicyValue);
            return;
        }

        // targetCustomerId가 없으면 family 전체(active customer)에게 반영
        List<FamilyMemberRepository.FamilyMemberTargetProjection> customers =
                familyMemberRepository.findAllActiveTargetsByFamilyId(payload.familyId());
        int appliedCount = 0;
        int skippedCount = 0;

        // family 구성원 단위로 동일 정책을 순차 반영
        for (FamilyMemberRepository.FamilyMemberTargetProjection customer : customers) {
            boolean applied =
                    processCustomerPolicyUpdate(
                            eventId,
                            customer.getFamilyId(),
                            customer.getCustomerId(),
                            policyKey,
                            eventVersion,
                            normalizedPolicyValue);
            if (applied) {
                appliedCount++;
            } else {
                skippedCount++;
            }
        }

        log.info(
                "Processed family-wide constraint. eventId={}, familyId={}, appliedCount={},"
                        + " skippedCount={}, field={}"
                        + VALUE_LOG_SUFFIX,
                logSanitizer.sanitize(eventId),
                payload.familyId(),
                appliedCount,
                skippedCount,
                logSanitizer.sanitize(policyKey),
                logSanitizer.sanitize(normalizedPolicyValue.normalizedNewValue()));
    }

    private void processGlobalPolicyUpdate(
            String eventId,
            String policyKey,
            long eventVersion,
            NormalizedPolicyValue normalizedPolicyValue) {
        List<FamilyMemberRepository.FamilyMemberTargetProjection> members =
                familyMemberRepository.findAllActiveTargets();
        AtomicInteger appliedCount = new AtomicInteger(0);
        AtomicInteger skippedCount = new AtomicInteger(0);

        members.parallelStream()
                .forEach(
                        member -> {
                            boolean applied =
                                    processCustomerPolicyUpdate(
                                            eventId,
                                            member.getFamilyId(),
                                            member.getCustomerId(),
                                            policyKey,
                                            eventVersion,
                                            normalizedPolicyValue);
                            if (applied) {
                                appliedCount.incrementAndGet();
                            } else {
                                skippedCount.incrementAndGet();
                            }
                        });

        log.info(
                "Processed global constraint. eventId={}, appliedCount={}, skippedCount={},"
                        + " field={}"
                        + VALUE_LOG_SUFFIX,
                logSanitizer.sanitize(eventId),
                appliedCount,
                skippedCount,
                logSanitizer.sanitize(policyKey),
                logSanitizer.sanitize(normalizedPolicyValue.normalizedNewValue()));
    }

    private NormalizedPolicyValue resolveNormalizedPolicyValue(
            PolicyUpdatedPayload payload, String eventId, Long targetCustomerId) {
        // 비활성화 정책이면 삭제(HDEL)로 처리되도록 null 값을 전달
        if (!payload.isActive()) {
            return new NormalizedPolicyValue(null, Set.of());
        }

        try {
            if (PolicyConstraintKeyConstants.BLOCK_APP.equals(payload.policyKey())) {
                // BLOCK:APP은 앱 목록 집합 + CSV 문자열을 함께 계산
                Set<String> normalizedBlockedApps =
                        policyConstraintEventMapper.normalizeAppBlockValueAsSet(payload.newValue());
                return new NormalizedPolicyValue(
                        String.join(",", normalizedBlockedApps), normalizedBlockedApps);
            }

            return new NormalizedPolicyValue(
                    policyConstraintEventMapper.normalizeValue(
                            payload.policyKey(), payload.newValue()),
                    Set.of());
        } catch (IllegalArgumentException e) {
            log.warn(
                    "Invalid policy value. eventId={}, familyId={}, customerId={}, field={},"
                            + " rawValue={}, reason={}",
                    logSanitizer.sanitize(eventId),
                    payload.familyId(),
                    targetCustomerId,
                    logSanitizer.sanitize(payload.policyKey()),
                    logSanitizer.sanitize(payload.newValue()),
                    logSanitizer.sanitize(e.getMessage()));
            return null;
        }
    }

    // family 내 유효한 customer인지 확인
    private boolean isValidFamilyMember(Long familyId, Long customerId) {
        return familyMemberRepository.existsByFamilyIdAndCustomerIdAndDeletedAtIsNull(
                familyId, customerId);
    }

    private boolean processCustomerPolicyUpdate(
            String eventId,
            Long familyId,
            Long customerId,
            String policyKey,
            long eventVersion,
            NormalizedPolicyValue normalizedPolicyValue) {
        // Redis constraints 키가 없으면 DB 기반으로 초기 워밍업
        policyConstraintWarmupHelper.warmupIfMissing(familyId, customerId);

        if (PolicyConstraintKeyConstants.BLOCK_APP.equals(policyKey)) {
            // BLOCK:APP은 현재 Redis 상태와 목표 앱 목록을 diff로 동기화
            boolean changed =
                    syncBlockedAppsToCustomer(
                            eventId,
                            familyId,
                            customerId,
                            normalizedPolicyValue.normalizedBlockedApps(),
                            eventVersion);
            logResult(
                    eventId,
                    familyId,
                    customerId,
                    policyKey,
                    normalizedPolicyValue.normalizedNewValue(),
                    changed ? LUA_RESULT_APPLIED : "NO_CHANGES");
            return changed;
        }

        // 일반 정책은 Lua 원자 연산으로 단건 반영
        String result =
                applyConstraintToCustomer(
                        eventId,
                        eventVersion,
                        familyId,
                        customerId,
                        policyKey,
                        normalizedPolicyValue.normalizedNewValue());
        logResult(
                eventId,
                familyId,
                customerId,
                policyKey,
                normalizedPolicyValue.normalizedNewValue(),
                result);
        return LUA_RESULT_APPLIED.equals(result);
    }

    private boolean syncBlockedAppsToCustomer(
            String eventId,
            Long familyId,
            Long customerId,
            Set<String> desiredBlockedApps,
            long eventVersion) {
        // 현재 Redis에 저장된 앱 차단 필드(BLOCK:APP:{appId})를 조회
        String constraintsKey =
                redisKeyGenerator.generateFamilyCustomerConstraintsKey(familyId, customerId);
        Set<String> currentBlockedApps = loadBlockedApps(constraintsKey);

        // 현재값 기준으로 삭제/추가 대상(diff)을 계산
        Set<String> appsToDelete = new LinkedHashSet<>(currentBlockedApps);
        appsToDelete.removeAll(desiredBlockedApps);

        Set<String> appsToAdd = new LinkedHashSet<>(desiredBlockedApps);
        appsToAdd.removeAll(currentBlockedApps);

        if (appsToDelete.isEmpty() && appsToAdd.isEmpty()) {
            return false;
        }

        int appliedCount = 0;

        // 앱별 Lua 실행으로 dedup/stale/version 검증을 동일하게 적용
        for (String appId : appsToDelete) {
            String appField = PolicyConstraintKeyConstants.BLOCK_APP_PREFIX + appId;
            String result =
                    applyConstraintToCustomer(
                            eventId + ":" + appField,
                            eventVersion,
                            familyId,
                            customerId,
                            appField,
                            null);
            if (LUA_RESULT_APPLIED.equals(result)) {
                appliedCount++;
            }
        }

        for (String appId : appsToAdd) {
            String appField = PolicyConstraintKeyConstants.BLOCK_APP_PREFIX + appId;
            String result =
                    applyConstraintToCustomer(
                            eventId + ":" + appField,
                            eventVersion,
                            familyId,
                            customerId,
                            appField,
                            "1");
            if (LUA_RESULT_APPLIED.equals(result)) {
                appliedCount++;
            }
        }

        return appliedCount > 0;
    }

    private Set<String> loadBlockedApps(String constraintsKey) {
        // constraints hash의 field 목록 중 BLOCK:APP: prefix만 추출해 앱 ID 집합으로 변환
        Set<Object> fields = familyStringRedisTemplate.opsForHash().keys(constraintsKey);
        if (fields.isEmpty()) {
            return Set.of();
        }

        return fields.stream()
                .map(String::valueOf)
                .filter(field -> field.startsWith(PolicyConstraintKeyConstants.BLOCK_APP_PREFIX))
                .map(
                        field ->
                                field.substring(
                                        PolicyConstraintKeyConstants.BLOCK_APP_PREFIX.length()))
                .filter(appId -> !appId.isBlank())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private String applyConstraintToCustomer(
            String eventId,
            long eventVersion,
            Long familyId,
            Long customerId,
            String policyKey,
            String newValue) {
        // dedupKey: 동일 이벤트 재처리 방지, constraintsKey: 실제 제약값 저장 키
        String dedupKey = redisKeyGenerator.generatePolicyEventDedupKey(eventId, customerId);
        String constraintsKey =
                redisKeyGenerator.generateFamilyCustomerConstraintsKey(familyId, customerId);
        // 삭제 이벤트는 빈 문자열로 정규화해 Lua 스크립트에서 일관되게 처리
        String normalizedNewValue = (newValue == null || newValue.isBlank()) ? "" : newValue;

        List<String> result;
        try {
            // Lua 스크립트가 dedup + stale + HSET/HDEL을 원자적으로 수행한다.
            result =
                    familyStringRedisTemplate.execute(
                            policyConstraintUpdateScript,
                            List.of(dedupKey, constraintsKey),
                            String.valueOf(dedupTtlSeconds),
                            policyKey,
                            normalizedNewValue,
                            String.valueOf(eventVersion));
        } catch (Exception e) {
            // Redis/Lua 실패는 비즈니스 예외로 전환해 상위에서 실패를 인지하게 함
            log.error(
                    "Failed to sync policy constraint to Redis. eventId={}, familyId={},"
                            + " customerId={}, field={}"
                            + VALUE_LOG_SUFFIX,
                    logSanitizer.sanitize(eventId),
                    familyId,
                    customerId,
                    logSanitizer.sanitize(policyKey),
                    logSanitizer.sanitize(newValue),
                    e);
            throw new ApplicationException(PolicyErrorCode.POLICY_REDIS_SYNC_FAILED);
        }
        // Lua 결과가 비정상이면 무시하지 않고 예외 처리
        if (result == null || result.isEmpty()) {
            log.error(
                    "Invalid Redis Lua result. eventId={}, familyId={}, customerId={}, field={}",
                    logSanitizer.sanitize(eventId),
                    familyId,
                    customerId,
                    logSanitizer.sanitize(policyKey));
            throw new ApplicationException(PolicyErrorCode.POLICY_REDIS_INVALID_RESULT);
        }

        return result.get(0);
    }

    private long resolveEventVersion(EventEnvelope<PolicyUpdatedPayload> envelope) {
        // timestamp가 없으면 현재 시각을 버전으로 대체
        if (envelope.timestamp() == null) {
            return System.currentTimeMillis();
        }
        // timestamp를 KST 기준 epoch millis로 변환해 Lua 버전 비교에 사용
        return envelope.timestamp().atZone(TimeConstants.ASIA_SEOUL).toInstant().toEpochMilli();
    }

    private void logResult(
            String eventId,
            Long familyId,
            Long customerId,
            String policyKey,
            String newValue,
            String result) {
        // APPLIED 외 값(중복/버전역전 등)은 skip 사유로 기록해 추적 가능하게 함
        if (LUA_RESULT_APPLIED.equals(result)) {
            log.info(
                    "Updated customer constraint. eventId={}, familyId={}, customerId={}, field={}"
                            + VALUE_LOG_SUFFIX,
                    logSanitizer.sanitize(eventId),
                    familyId,
                    customerId,
                    logSanitizer.sanitize(policyKey),
                    logSanitizer.sanitize(newValue));
            return;
        }

        log.info(
                "Skipped customer constraint update. eventId={}, familyId={}, customerId={},"
                        + " field={}, reason={}",
                logSanitizer.sanitize(eventId),
                familyId,
                customerId,
                logSanitizer.sanitize(policyKey),
                logSanitizer.sanitize(result));
    }

    private record NormalizedPolicyValue(
            String normalizedNewValue, Set<String> normalizedBlockedApps) {}
}
