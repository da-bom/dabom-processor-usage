package com.project.domain.usage.infra.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
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
    private static final String EVENT_TYPE_FIELD = "eventType";
    private static final String USAGE_PERSIST_EVENT_TYPE = "USAGE_PERSIST";
    private static final int MAX_LOG_VALUE_LENGTH = 128;

    private final ObjectMapper objectMapper;
    private final UsagePersistService usagePersistService;

    @KafkaListener(topics = "usage-persist", groupId = "dabom-processor-usage-persistence-group")
    public void consume(ConsumerRecord<String, String> consumerRecord) {
        try {
            JsonNode root = objectMapper.readTree(consumerRecord.value());
            String eventType = root.path(EVENT_TYPE_FIELD).asText("");
            if (!USAGE_PERSIST_EVENT_TYPE.equals(eventType)) {
                log.warn(
                        "Skip non-persist event on usage-persist topic. recordKey={}, eventType={}",
                        consumerRecord.key(),
                        sanitizeForLog(eventType));
                return;
            }

            EventEnvelope<UsagePersistPayload> envelope =
                    objectMapper.convertValue(root, new TypeReference<>() {});
            usagePersistService.persist(envelope, consumerRecord.key());

        } catch (JsonProcessingException e) {
            log.error("Failed to parse usage-persist payload", e);
        } catch (Exception e) {
            log.error("Failed to handle usage-persist event", e);
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
