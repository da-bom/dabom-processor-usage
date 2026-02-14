package com.project.domain.policy.infra.messaging;

import java.time.Duration;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.domain.family.entity.FamilyMember;
import com.project.domain.family.repository.FamilyMemberRepository;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.policy.PolicyUpdatedPayload;
import com.project.global.util.RedisKeyGenerator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class PolicyKafkaConsumer {
    private static final Pattern HHMM_PATTERN = Pattern.compile("^\\d{4}$");

    private final ObjectMapper objectMapper;
    private final RedisTemplate<String, String> familyStringRedisTemplate;
    private final RedisKeyGenerator redisKeyGenerator;
    private final FamilyMemberRepository familyMemberRepository;

    @Value("${app.kafka.dedup.policy-ttl-seconds}")
    private long dedupTtlSeconds;

    @KafkaListener(topics = "policy-updated", groupId = "dabom-processor-usage-policy-group")
    public void consume(ConsumerRecord<String, String> record) {
        try {
            // 역직렬화
            EventEnvelope<PolicyUpdatedPayload> envelope =
                    objectMapper.readValue(record.value(), new TypeReference<>() {});
            PolicyUpdatedPayload payload = envelope.payload();
            String eventId = envelope.eventId();

            // payload/eventId/필수값 검증
            if (!isValidPayload(payload, eventId, record)) {
                return;
            }

            // 중복 이벤트는 스킵
            if (isDuplicated(eventId)) {
                return;
            }

            String policyKey = payload.policyKey();
            String newValue = payload.newValue();
            Long targetCustomerId = payload.targetCustomerId();

            if (!isAllowedPolicyKey(policyKey)) {
                log.warn(
                        "Invalid policy key. eventId={}, familyId={}, customerId={}, field={}",
                        eventId,
                        payload.familyId(),
                        targetCustomerId,
                        policyKey);
                return;
            }

            // newValue가 있는 경우에만 값 형식 검증
            if (newValue != null
                    && !newValue.isBlank()
                    && !isValidPolicyValue(policyKey, newValue)) {
                log.warn(
                        "Invalid policy value. eventId={}, familyId={}, customerId={}, field={},"
                                + " value={}",
                        eventId,
                        payload.familyId(),
                        targetCustomerId,
                        policyKey,
                        newValue);
                return;
            }

            // targetCustomerId가 있으면 해당 customer만 반영
            if (targetCustomerId != null) {
                applyConstraintToCustomer(
                        payload.familyId(), targetCustomerId, policyKey, newValue);
                log.info(
                        "Updated customer constraint. eventId={}, familyId={}, customerId={},"
                                + " field={}, value={}",
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
                    "Updated family-wide constraint. eventId={}, familyId={}, targetCount={},"
                            + " field={}, value={}",
                    eventId,
                    payload.familyId(),
                    customers.size(),
                    policyKey,
                    newValue);
        } catch (JsonProcessingException e) {
            log.error("Failed to parse policy-updated payload", e);
        } catch (Exception e) {
            log.error("Failed to handle policy-updated event", e);
        }
    }

    private boolean isValidPayload(
            PolicyUpdatedPayload payload, String eventId, ConsumerRecord<String, String> record) {
        // payload가 없으면 종료
        if (payload == null) {
            log.warn("policy-updated payload is null. recordKey={}", record.key());
            return false;
        }

        // eventId가 없으면 멱등 처리 불가
        if (eventId == null || eventId.isBlank()) {
            log.warn(
                    "policy-updated eventId is empty. familyId={}, customerId={}, policyKey={}",
                    payload.familyId(),
                    payload.targetCustomerId(),
                    payload.policyKey());
            return false;
        }

        // familyId/policyKey는 필수값
        if (payload.familyId() == null
                || payload.policyKey() == null
                || payload.policyKey().isBlank()) {
            log.warn(
                    "Invalid policy-updated payload. eventId={}, familyId={}, customerId={},"
                            + " policyKey={}",
                    eventId,
                    payload.familyId(),
                    payload.targetCustomerId(),
                    payload.policyKey());
            return false;
        }

        return true;
    }

    private boolean isDuplicated(String eventId) {
        // dedup 키가 이미 있으면 중복 이벤트로 판단
        String dedupKey = "event:dedup:policy:" + eventId;
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
        // customer 단위 constraints 키 생성
        String constraintsKey =
                redisKeyGenerator.generateFamilyCustomerConstraintsKey(familyId, customerId);
        applyConstraint(constraintsKey, policyKey, newValue);
    }

    private void applyConstraint(String constraintsKey, String policyKey, String newValue) {
        // newValue가 비어있으면 정책 해제(HDEL)
        if (newValue == null || newValue.isBlank()) {
            familyStringRedisTemplate.opsForHash().delete(constraintsKey, policyKey);
            return;
        }
        // newValue가 있으면 정책 갱신(HSET)
        familyStringRedisTemplate.opsForHash().put(constraintsKey, policyKey, newValue);
    }

    private boolean isAllowedPolicyKey(String policyKey) {
        // 삭제/갱신 공통으로 허용된 정책 키인지 먼저 검증
        if (policyKey == null || policyKey.isBlank()) {
            return false;
        }
        if ("THROTTLE:SPEED".equals(policyKey)) {
            return true;
        }
        if ("BLOCK:ACCESS".equals(policyKey) || policyKey.startsWith("BLOCK:APP:")) {
            return true;
        }
        if ("BLOCK:TIME:START".equals(policyKey) || "BLOCK:TIME:END".equals(policyKey)) {
            return true;
        }
        if (policyKey.startsWith("LIMIT:DATA:")) {
            return true;
        }
        return false;
    }

    private boolean isValidPolicyValue(String policyKey, String newValue) {
        // 정책 키별 value 형식 검증
        if ("THROTTLE:SPEED".equals(policyKey)) {
            return isPositiveLong(newValue);
        }
        if ("BLOCK:ACCESS".equals(policyKey) || policyKey.startsWith("BLOCK:APP:")) {
            return "1".equals(newValue) || "0".equals(newValue);
        }
        if ("BLOCK:TIME:START".equals(policyKey) || "BLOCK:TIME:END".equals(policyKey)) {
            return isValidHhmm(newValue);
        }
        if (policyKey.startsWith("LIMIT:DATA:")) {
            return isPositiveLong(newValue);
        }
        return false;
    }

    private boolean isPositiveLong(String value) {
        // 양의 정수 검증
        try {
            return Long.parseLong(value) > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean isValidHhmm(String value) {
        // HHMM 형식 + 시/분 범위 검증
        if (!HHMM_PATTERN.matcher(value).matches()) {
            return false;
        }
        int hh = Integer.parseInt(value.substring(0, 2));
        int mm = Integer.parseInt(value.substring(2, 4));
        return hh >= 0 && hh <= 23 && mm >= 0 && mm <= 59;
    }
}
