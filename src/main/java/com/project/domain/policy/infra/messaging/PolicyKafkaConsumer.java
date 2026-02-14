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

    // 중복 이벤트 키 TTL 설정 (기본값: 3600초)
    @Value("${app.kafka.dedup.policy-ttl-seconds}")
    private long dedupTtlSeconds;

    @KafkaListener(topics = "policy-updated", groupId = "dabom-processor-usage-policy-group")
    public void consume(ConsumerRecord<String, String> record) {
        try {
            // 역직렬화
            EventEnvelope<PolicyUpdatedPayload> envelope =
                    objectMapper.readValue(record.value(), new TypeReference<>() {});

            // payload가 없으면 로그 출력 후 종료
            PolicyUpdatedPayload payload = envelope.payload();
            if (payload == null) {
                log.warn("policy-updated payload is null. recordKey={}", record.key());
                return;
            }

            // eventId가 비어있으면 멱등 처리 불가라서 로그 출력 후 종료
            String eventId = envelope.eventId();
            if (eventId == null || eventId.isBlank()) {
                log.warn(
                        "policy-updated eventId is empty. familyId={}, customerId={}, policyKey={}",
                        payload.familyId(),
                        payload.targetCustomerId(),
                        payload.policyKey());
                return;
            }

            // 중복 방지: 이미 키가 있으면 중복 이벤트로 판단하고 스킵
            String dedupKey = "event:dedup:policy:" + eventId;
            Boolean firstSeen =
                    familyStringRedisTemplate
                            .opsForValue()
                            .setIfAbsent(dedupKey, "1", Duration.ofSeconds(dedupTtlSeconds));
            if (!Boolean.TRUE.equals(firstSeen)) {
                log.info("Skip duplicated policy-updated event. eventId={}", eventId);
                return;
            }

            // payload 필수값 검증
            // 누락시 잘못된 이벤트로 간주하고 종료
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
                return;
            }

            String newValue = payload.newValue();
            Long targetCustomerId = payload.targetCustomerId();
            if (newValue != null && !newValue.isBlank()) {
                if (!isValidPolicyValue(payload.policyKey(), newValue)) {
                    log.warn(
                            "Invalid policy value. eventId={}, familyId={}, customerId={},"
                                    + " field={}, value={}",
                            eventId,
                            payload.familyId(),
                            targetCustomerId,
                            payload.policyKey(),
                            newValue);
                    return;
                }
            }

            // targetCustomerId != null: 해당하는 customer 정책 적용
            if (targetCustomerId != null) {
                String constraintsKey =
                        redisKeyGenerator.generateFamilyCustomerConstraintsKey(
                                payload.familyId(), targetCustomerId);
                applyConstraint(constraintsKey, payload.policyKey(), newValue);
                log.info(
                        "Updated customer constraint. eventId={}, familyId={}, customerId={},"
                                + " field={}, value={}",
                        eventId,
                        payload.familyId(),
                        targetCustomerId,
                        payload.policyKey(),
                        newValue);
                return;
            }

            // targetCustomerId == null: family 전체 정책 -> active customer 전원에게 반영
            List<FamilyMember> customers =
                    familyMemberRepository.findAllByFamilyIdAndDeletedAtIsNull(payload.familyId());

            for (FamilyMember customer : customers) {
                String constraintsKey =
                        redisKeyGenerator.generateFamilyCustomerConstraintsKey(
                                payload.familyId(), customer.getCustomerId());
                applyConstraint(constraintsKey, payload.policyKey(), newValue);
            }

            log.info(
                    "Updated family-wide constraint. eventId={}, familyId={}, targetCount={},"
                            + " field={}, value={}",
                    eventId,
                    payload.familyId(),
                    customers.size(),
                    payload.policyKey(),
                    newValue);
        } catch (JsonProcessingException e) {
            log.error("Failed to parse policy-updated payload", e);
        } catch (Exception e) {
            log.error("Failed to handle policy-updated event", e);
        }
    }

    private void applyConstraint(String constraintsKey, String policyKey, String newValue) {
        // newValue가 비어있으면 해당 policyKey 필드를 삭제 (정책 해제)
        if (newValue == null || newValue.isBlank()) {
            familyStringRedisTemplate.opsForHash().delete(constraintsKey, policyKey);
            return;
        }
        // newValue가 있으면 해당 policyKey 필드를 새 값으로 저장 (정책 갱신)
        familyStringRedisTemplate.opsForHash().put(constraintsKey, policyKey, newValue);
    }

    // value 값 검증
    private boolean isValidPolicyValue(String policyKey, String newValue) {
        if (policyKey == null || policyKey.isBlank()) {
            return false;
        }
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

        // 알 수 없는 정책 키는 v1에서는 허용하지 않음
        return false;
    }

    private boolean isPositiveLong(String value) {
        try {
            return Long.parseLong(value) > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean isValidHhmm(String value) {
        if (!HHMM_PATTERN.matcher(value).matches()) {
            return false;
        }
        int hh = Integer.parseInt(value.substring(0, 2));
        int mm = Integer.parseInt(value.substring(2, 4));
        return hh >= 0 && hh <= 23 && mm >= 0 && mm <= 59;
    }
}
