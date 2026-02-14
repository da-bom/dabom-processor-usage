package com.project.domain.family.infra.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.usage.UsagePayload;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class FamilyKafkaConsumer {

    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "family-events", groupId = "family-group")
    public void consume(ConsumerRecord<String, String> record) {
        try {
            EventEnvelope<UsagePayload> envelope =
                    objectMapper.readValue(
                            record.value(), new TypeReference<EventEnvelope<UsagePayload>>() {});

            UsagePayload payload = envelope.payload();

            log.info(
                    "Handling family event: familyId={}, customerId={}",
                    payload.familyId(),
                    payload.customerId());
        } catch (JsonProcessingException e) {
            log.error("Failed to parse family event payload", e);
        }
    }
}
