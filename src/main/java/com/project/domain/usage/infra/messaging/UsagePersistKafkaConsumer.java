package com.project.domain.usage.infra.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.project.domain.usage.service.UsagePersistService;
import com.project.global.event.KafkaEventMessageSupport;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.usage.UsagePersistPayload;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class UsagePersistKafkaConsumer {
    private static final String USAGE_PERSIST_EVENT_TYPE = "USAGE_PERSIST";

    private final KafkaEventMessageSupport kafkaEventMessageSupport;
    private final UsagePersistService usagePersistService;

    @KafkaListener(topics = "usage-persist", groupId = "dabom-processor-usage-persistence-group")
    public void consume(ConsumerRecord<String, String> consumerRecord) {
        try {
            // 메시지에서 eventType 확인 후 정상 메시지만 변환
            JsonNode root = kafkaEventMessageSupport.readTree(consumerRecord.value());
            String eventType = kafkaEventMessageSupport.extractEventType(root);
            if (!USAGE_PERSIST_EVENT_TYPE.equals(eventType)) {
                log.warn(
                        "Skip non-persist event on usage-persist topic. recordKey={}, eventType={}",
                        consumerRecord.key(),
                        kafkaEventMessageSupport.sanitizeForLog(eventType));
                return;
            }

            EventEnvelope<UsagePersistPayload> envelope =
                    kafkaEventMessageSupport.convertToEnvelope(root, new TypeReference<>() {});
            usagePersistService.persist(envelope, consumerRecord.key());

        } catch (JsonProcessingException e) {
            log.error("Failed to parse usage-persist payload", e);
        } catch (Exception e) {
            log.error("Failed to handle usage-persist event", e);
        }
    }
}
