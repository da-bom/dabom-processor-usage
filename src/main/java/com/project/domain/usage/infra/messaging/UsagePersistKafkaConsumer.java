package com.project.domain.usage.infra.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.domain.usage.service.UsagePersistService;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.usage.UsagePersistPayload;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class UsagePersistKafkaConsumer {

    private final ObjectMapper objectMapper;
    private final UsagePersistService usagePersistService;

    @KafkaListener(topics = "usage-persist", groupId = "dabom-processor-usage-persistence-group")
    public void consume(ConsumerRecord<String, String> consumerRecord) {
        try {
            EventEnvelope<UsagePersistPayload> envelope =
                    objectMapper.readValue(consumerRecord.value(), new TypeReference<>() {});
            usagePersistService.persist(envelope, consumerRecord.key());

        } catch (JsonProcessingException e) {
            log.error("Failed to parse usage-persist payload", e);
        } catch (Exception e) {
            log.error("Failed to handle usage-persist event", e);
        }
    }
}
