package com.project.domain.policy.service;

import java.time.Duration;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import com.project.domain.family.entity.FamilyMember;
import com.project.domain.family.repository.FamilyMemberRepository;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.policy.PolicyUpdatedPayload;
import com.project.global.util.RedisKeyGenerator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class PolicyConstraintSyncService {
    private static final String VALUE_LOG_SUFFIX = ", value={}";

    private final RedisTemplate<String, String> familyStringRedisTemplate;
    private final RedisKeyGenerator redisKeyGenerator;
    private final FamilyMemberRepository familyMemberRepository;
    private final PolicyEventValidator policyEventValidator;

    @Value("${app.kafka.dedup.policy-ttl-seconds}")
    private long dedupTtlSeconds;

    public void sync(EventEnvelope<PolicyUpdatedPayload> envelope, String recordKey) {
        PolicyUpdatedPayload payload = envelope.payload();
        String eventId = envelope.eventId();

        // payload/eventId/필수값 검증
        if (!policyEventValidator.isValidPayload(payload, eventId, recordKey)) {
            return;
        }

        // 중복 이벤트는 스킵
        if (isDuplicated(eventId)) {
            return;
        }

        String policyKey = payload.policyKey();
        String newValue = payload.newValue();
        Long targetCustomerId = payload.targetCustomerId();

        // 삭제/갱신 모두 policyKey whitelist 검증
        if (!policyEventValidator.isAllowedPolicyKey(policyKey)) {
            log.warn(
                    "Invalid policy key. eventId={}, familyId={}, customerId={}, field={}",
                    eventId,
                    payload.familyId(),
                    targetCustomerId,
                    policyKey);
            return;
        }

        // 갱신일 때만 값 형식 검증
        if (newValue != null
                && !newValue.isBlank()
                && !policyEventValidator.isValidPolicyValue(policyKey, newValue)) {
            log.warn(
                    "Invalid policy value. eventId={}, familyId={}, customerId={}, field={}"
                            + VALUE_LOG_SUFFIX,
                    eventId,
                    payload.familyId(),
                    targetCustomerId,
                    policyKey,
                    newValue);
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
                        eventId,
                        payload.familyId(),
                        targetCustomerId,
                        policyKey);
                return;
            }

            applyConstraintToCustomer(payload.familyId(), targetCustomerId, policyKey, newValue);
            log.info(
                    "Updated customer constraint. eventId={}, familyId={}, customerId={}, field={}"
                            + VALUE_LOG_SUFFIX,
                    eventId,
                    payload.familyId(),
                    targetCustomerId,
                    policyKey,
                    newValue);
            return;
        }

        // targetCustomerId가 없으면 family 전체(active customer)에게 반영
        List<FamilyMember> customers =
                familyMemberRepository.findAllByFamilyIdAndDeletedAtIsNull(payload.familyId());
        for (FamilyMember customer : customers) {
            applyConstraintToCustomer(
                    payload.familyId(), customer.getCustomerId(), policyKey, newValue);
        }

        log.info(
                "Updated family-wide constraint. eventId={}, familyId={}, targetCount={}, field={}"
                        + VALUE_LOG_SUFFIX,
                eventId,
                payload.familyId(),
                customers.size(),
                policyKey,
                newValue);
    }

    private boolean isDuplicated(String eventId) {
        String dedupKey = redisKeyGenerator.generatePolicyEventDedupKey(eventId);
        Boolean firstSeen =
                familyStringRedisTemplate
                        .opsForValue()
                        .setIfAbsent(dedupKey, "1", Duration.ofSeconds(dedupTtlSeconds));

        if (!Boolean.TRUE.equals(firstSeen)) {
            log.info("Skip duplicated policy-updated event. eventId={}", eventId);
            return true;
        }

        return false;
    }

    private void applyConstraintToCustomer(
            Long familyId, Long customerId, String policyKey, String newValue) {
        String constraintsKey =
                redisKeyGenerator.generateFamilyCustomerConstraintsKey(familyId, customerId);
        applyConstraint(constraintsKey, policyKey, newValue);
    }

    private void applyConstraint(String constraintsKey, String policyKey, String newValue) {
        if (newValue == null || newValue.isBlank()) {
            familyStringRedisTemplate.opsForHash().delete(constraintsKey, policyKey);
            return;
        }
        familyStringRedisTemplate.opsForHash().put(constraintsKey, policyKey, newValue);
    }
}
