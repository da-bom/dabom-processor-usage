package com.project.domain.policy.infra.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.dabom.messaging.kafka.contract.KafkaConsumerGroups;
import com.dabom.messaging.kafka.contract.KafkaEventTypes;
import com.dabom.messaging.kafka.contract.KafkaTopics;
import com.dabom.messaging.kafka.event.KafkaEventMessageSupport;
import com.dabom.messaging.kafka.event.consumer.KafkaEventConsumer;
import com.dabom.messaging.kafka.event.dto.EventEnvelope;
import com.dabom.messaging.kafka.event.dto.policy.PolicyUpdatedPayload;
import com.fasterxml.jackson.core.type.TypeReference;
import com.project.domain.policy.service.PolicyConstraintSyncService;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class PolicyKafkaConsumer implements KafkaEventConsumer<PolicyUpdatedPayload> {

    private final KafkaEventMessageSupport kafkaEventMessageSupport;
    private final PolicyConstraintSyncService policyConstraintSyncService;

    @KafkaListener(
            topics = KafkaTopics.POLICY_UPDATED,
            groupId = KafkaConsumerGroups.DABOM_PROCESSOR_USAGE_POLICY)
    public void consume(ConsumerRecord<String, String> consumerRecord) {
        consume(consumerRecord, kafkaEventMessageSupport);
    }

    @Override
    public String eventType() {
        return KafkaEventTypes.POLICY_UPDATED;
    }

    @Override
    public TypeReference<EventEnvelope<PolicyUpdatedPayload>> typeReference() {
        return new TypeReference<>() {};
    }

    @Override
    public void handle(EventEnvelope<PolicyUpdatedPayload> envelope, String recordKey) {
        policyConstraintSyncService.sync(envelope, recordKey);
    }
}
