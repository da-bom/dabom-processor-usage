package com.project.domain.policy.service;

import java.time.ZoneOffset;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import com.project.domain.family.entity.FamilyMember;
import com.project.domain.family.repository.FamilyMemberRepository;
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

        // 이벤트 payload에서 정책 키/값/대상을 추출한다.
        String policyKey = payload.policyKey();
        String newValue = payload.newValue();
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

        String normalizedNewValue;
        try {
            normalizedNewValue = policyConstraintEventMapper.normalizeValue(policyKey, newValue);
        } catch (IllegalArgumentException e) {
            log.warn(
                    "Invalid policy value. eventId={}, familyId={}, customerId={}, field={},"
                            + " rawValue={}, reason={}",
                    logSanitizer.sanitize(eventId),
                    payload.familyId(),
                    targetCustomerId,
                    logSanitizer.sanitize(policyKey),
                    logSanitizer.sanitize(newValue),
                    logSanitizer.sanitize(e.getMessage()));
            return;
        }

        // targetCustomerId가 있으면 해당 customer만 반영
        if (targetCustomerId != null) {
            // family-customer 소속 관계 검증
            boolean isFamilyMember =
                    familyMemberRepository.existsByFamilyIdAndCustomerIdAndDeletedAtIsNull(
                            payload.familyId(), targetCustomerId);
            if (!isFamilyMember) {
                log.warn(
                        "Skip policy update due to invalid family-customer relation."
                                + " eventId={}, familyId={}, customerId={}, field={}",
                        logSanitizer.sanitize(eventId),
                        payload.familyId(),
                        targetCustomerId,
                        logSanitizer.sanitize(policyKey));
                return;
            }

            // Redis constraints 키가 없으면 DB 기반으로 초기 워밍업한다.
            policyConstraintWarmupHelper.warmupIfMissing(payload.familyId(), targetCustomerId);

            String result =
                    applyConstraintToCustomer(
                            eventId,
                            eventVersion,
                            payload.familyId(),
                            targetCustomerId,
                            policyKey,
                            normalizedNewValue);
            logResult(
                    eventId,
                    payload.familyId(),
                    targetCustomerId,
                    policyKey,
                    normalizedNewValue,
                    result);
            return;
        }

        // targetCustomerId가 없으면 family 전체(active customer)에게 반영
        List<FamilyMember> customers =
                familyMemberRepository.findAllByFamilyIdAndDeletedAtIsNull(payload.familyId());
        int appliedCount = 0;
        int skippedCount = 0;
        // family 구성원 단위로 동일 정책을 순차 반영
        for (FamilyMember customer : customers) {
            // 각 customer별 constraints 키 부재 시 DB 값을 기반으로 복구한다.
            policyConstraintWarmupHelper.warmupIfMissing(
                    payload.familyId(), customer.getCustomerId());

            String result =
                    applyConstraintToCustomer(
                            eventId,
                            eventVersion,
                            payload.familyId(),
                            customer.getCustomerId(),
                            policyKey,
                            normalizedNewValue);
            if (LUA_RESULT_APPLIED.equals(result)) {
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
                logSanitizer.sanitize(normalizedNewValue));
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
        // OffsetDateTime -> epoch millis로 변환해 Lua 버전 비교에 사용한다.
        return envelope.timestamp().toInstant(ZoneOffset.UTC).toEpochMilli();
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
}
