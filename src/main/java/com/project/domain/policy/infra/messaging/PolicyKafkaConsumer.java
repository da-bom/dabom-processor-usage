package com.project.domain.policy.infra.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.project.domain.policy.service.PolicyConstraintSyncService;
import com.project.global.event.KafkaEventMessageSupport;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class PolicyKafkaConsumer {
    private static final String POLICY_UPDATED_EVENT_TYPE = "POLICY_UPDATED";

    private final KafkaEventMessageSupport kafkaEventMessageSupport;
    private final PolicyConstraintSyncService policyConstraintSyncService;

    @KafkaListener(topics = "policy-updated", groupId = "dabom-processor-usage-policy-group")
    public void consume(ConsumerRecord<String, String> consumerRecord) {
        // eventType 필터링/역직렬화/예외 처리는 공통 유틸에서 수행한다.
        kafkaEventMessageSupport.consumeByEventType(
                consumerRecord,
                POLICY_UPDATED_EVENT_TYPE,
                new TypeReference<>() {},
                policyConstraintSyncService::sync);
    }
}
