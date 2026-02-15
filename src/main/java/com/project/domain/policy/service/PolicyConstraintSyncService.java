package com.project.domain.policy.service;

import java.time.ZoneOffset;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
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
    private final RedisScript<List> policyConstraintUpdateScript;
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

        String policyKey = payload.policyKey();
        String newValue = payload.newValue();
        Long targetCustomerId = payload.targetCustomerId();
        long eventVersion = resolveEventVersion(envelope);

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

            String result =
                    applyConstraintToCustomer(
                            eventId,
                            eventVersion,
                            payload.familyId(),
                            targetCustomerId,
                            policyKey,
                            newValue);
            logResult(eventId, payload.familyId(), targetCustomerId, policyKey, newValue, result);
            return;
        }

        // targetCustomerId가 없으면 family 전체(active customer)에게 반영
        List<FamilyMember> customers =
                familyMemberRepository.findAllByFamilyIdAndDeletedAtIsNull(payload.familyId());
        int appliedCount = 0;
        int skippedCount = 0;
        for (FamilyMember customer : customers) {
            String result =
                    applyConstraintToCustomer(
                            eventId,
                            eventVersion,
                            payload.familyId(),
                            customer.getCustomerId(),
                            policyKey,
                            newValue);
            if ("APPLIED".equals(result)) {
                appliedCount++;
            } else {
                skippedCount++;
            }
        }

        log.info(
                "Processed family-wide constraint. eventId={}, familyId={}, appliedCount={},"
                        + " skippedCount={}, field={}"
                        + VALUE_LOG_SUFFIX,
                eventId,
                payload.familyId(),
                appliedCount,
                skippedCount,
                policyKey,
                newValue);
    }

    private String applyConstraintToCustomer(
            String eventId,
            long eventVersion,
            Long familyId,
            Long customerId,
            String policyKey,
            String newValue) {
        String dedupKey = redisKeyGenerator.generatePolicyEventDedupKey(eventId, customerId);
        String constraintsKey =
                redisKeyGenerator.generateFamilyCustomerConstraintsKey(familyId, customerId);
        String versionKey =
                redisKeyGenerator.generateFamilyCustomerConstraintsVersionKey(familyId, customerId);
        String normalizedNewValue = (newValue == null || newValue.isBlank()) ? "" : newValue;

        List result =
                familyStringRedisTemplate.execute(
                        policyConstraintUpdateScript,
                        List.of(dedupKey, constraintsKey, versionKey),
                        String.valueOf(dedupTtlSeconds),
                        policyKey,
                        normalizedNewValue,
                        String.valueOf(eventVersion));
        if (result.isEmpty()) {
            return "UNKNOWN";
        }
        return String.valueOf(result.getFirst());
    }

    private long resolveEventVersion(EventEnvelope<PolicyUpdatedPayload> envelope) {
        if (envelope.timestamp() == null) {
            return System.currentTimeMillis();
        }
        return envelope.timestamp().toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    private void logResult(
            String eventId,
            Long familyId,
            Long customerId,
            String policyKey,
            String newValue,
            String result) {
        if ("APPLIED".equals(result)) {
            log.info(
                    "Updated customer constraint. eventId={}, familyId={}, customerId={}, field={}"
                            + VALUE_LOG_SUFFIX,
                    eventId,
                    familyId,
                    customerId,
                    policyKey,
                    newValue);
            return;
        }

        log.info(
                "Skipped customer constraint update. eventId={}, familyId={}, customerId={},"
                        + " field={}, reason={}",
                eventId,
                familyId,
                customerId,
                policyKey,
                result);
    }
}
