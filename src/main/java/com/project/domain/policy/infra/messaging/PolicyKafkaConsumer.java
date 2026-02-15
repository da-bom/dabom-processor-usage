package com.project.domain.policy.infra.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.domain.policy.service.PolicyConstraintSyncService;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.policy.PolicyUpdatedPayload;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class PolicyKafkaConsumer {
    private static final String EVENT_TYPE_FIELD = "eventType";
    private static final String POLICY_UPDATED_EVENT_TYPE = "POLICY_UPDATED";
    private static final int MAX_LOG_VALUE_LENGTH = 128;

    private final ObjectMapper objectMapper;
    private final PolicyConstraintSyncService policyConstraintSyncService;

    @KafkaListener(topics = "policy-updated", groupId = "dabom-processor-usage-policy-group")
    public void consume(ConsumerRecord<String, String> consumerRecord) {
        try {
            JsonNode root = objectMapper.readTree(consumerRecord.value());
            String eventType = root.path(EVENT_TYPE_FIELD).asText("");
            if (!POLICY_UPDATED_EVENT_TYPE.equals(eventType)) {
                log.warn(
                        "Skip non-policy event on policy-updated topic. recordKey={}, eventType={}",
                        consumerRecord.key(),
                        sanitizeForLog(eventType));
                return;
            }

            EventEnvelope<PolicyUpdatedPayload> envelope =
                    objectMapper.convertValue(root, new TypeReference<>() {});
            policyConstraintSyncService.sync(envelope, consumerRecord.key());
        } catch (JsonProcessingException e) {
            log.error("Failed to parse policy-updated payload", e);
        } catch (Exception e) {
            log.error("Failed to handle policy-updated event", e);
        }
    }

    private String sanitizeForLog(String raw) {
        if (raw == null) {
            return "null";
        }
        String sanitized = raw.replace('\r', '_').replace('\n', '_').replace('\t', '_');
        if (sanitized.length() > MAX_LOG_VALUE_LENGTH) {
            return sanitized.substring(0, MAX_LOG_VALUE_LENGTH) + "...";
        }
        return sanitized;
    }
}
