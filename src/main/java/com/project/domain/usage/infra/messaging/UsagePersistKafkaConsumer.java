package com.project.domain.usage.infra.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.dabom.messaging.kafka.contract.KafkaConsumerGroups;
import com.dabom.messaging.kafka.contract.KafkaEventTypes;
import com.dabom.messaging.kafka.contract.KafkaTopics;
import com.dabom.messaging.kafka.event.KafkaEventMessageSupport;
import com.dabom.messaging.kafka.event.consumer.KafkaEventConsumer;
import com.dabom.messaging.kafka.event.dto.EventEnvelope;
import com.dabom.messaging.kafka.event.dto.usage.UsagePersistPayload;
import com.fasterxml.jackson.core.type.TypeReference;
import com.project.domain.usage.service.UsagePersistService;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class UsagePersistKafkaConsumer implements KafkaEventConsumer<UsagePersistPayload> {

    private final KafkaEventMessageSupport kafkaEventMessageSupport;
    private final UsagePersistService usagePersistService;

    @KafkaListener(
            topics = KafkaTopics.USAGE_PERSIST,
            groupId = KafkaConsumerGroups.DABOM_PROCESSOR_USAGE_PERSISTENCE)
    public void consume(ConsumerRecord<String, String> consumerRecord) {
        consume(consumerRecord, kafkaEventMessageSupport);
    }

    @Override
    public String eventType() {
        return KafkaEventTypes.USAGE_PERSIST;
    }

    @Override
    public TypeReference<EventEnvelope<UsagePersistPayload>> typeReference() {
        return new TypeReference<>() {};
    }

    @Override
    public void handle(EventEnvelope<UsagePersistPayload> envelope, String recordKey) {
        usagePersistService.persist(envelope, recordKey);
    }
}
