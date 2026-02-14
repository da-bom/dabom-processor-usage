package com.project.domain.policy.infra.messaging;

import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.policy.PolicyUpdatedPayload;
import com.project.global.util.RedisKeyGenerator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class PolicyKafkaConsumer {

    private final ObjectMapper objectMapper;
    private final RedisTemplate<String, String> familyStringRedisTemplate;
    private final RedisKeyGenerator redisKeyGenerator;

    // 중복 이벤트 키 TTL 설정 (기본값: 3600초)
    @Value("${app.kafka.dedup.policy-ttl-seconds}")
    private long dedupTtlSeconds;

    @KafkaListener(topics = "policy-updated", groupId = "policy-group")
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
                    || payload.targetCustomerId() == null
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

            // constraint 키 생성
            String constraintsKey =
                    redisKeyGenerator.generateFamilyCustomerConstraintsKey(
                            payload.familyId(), payload.targetCustomerId());

            // 정책 반영
            // newValue가 비어있으면 해당 policyKey 필드를 삭제 (정책 해제)
            String newValue = payload.newValue();
            if (newValue == null || newValue.isBlank()) {
                familyStringRedisTemplate.opsForHash().delete(constraintsKey, payload.policyKey());
                log.info(
                        "Removed constraint. eventId={}, key={}, field={}",
                        eventId,
                        constraintsKey,
                        payload.policyKey());
                return;
            }

            // newValue가 있으면 해당 policyKey 필드를 새 값으로 저장 (정책 갱신)
            familyStringRedisTemplate
                    .opsForHash()
                    .put(constraintsKey, payload.policyKey(), newValue);
            log.info(
                    "Updated constraint. eventId={}, key={}, field={}, value={}",
                    eventId,
                    constraintsKey,
                    payload.policyKey(),
                    newValue);
        } catch (JsonProcessingException e) {
            log.error("Failed to parse policy-updated payload", e);
        } catch (Exception e) {
            log.error("Failed to handle policy-updated event", e);
        }
    }
}
