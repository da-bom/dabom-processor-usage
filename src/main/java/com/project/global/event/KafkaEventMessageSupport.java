package com.project.global.event;

import java.util.function.BiConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.util.LogSanitizer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventMessageSupport {
    private static final String EVENT_TYPE_FIELD = "eventType";

    private final ObjectMapper objectMapper;
    private final LogSanitizer logSanitizer;

    public JsonNode readTree(String rawMessage) throws JsonProcessingException {
        return objectMapper.readTree(rawMessage);
    }

    public String extractEventType(JsonNode root) {
        return root.path(EVENT_TYPE_FIELD).asText("");
    }

    public <T> EventEnvelope<T> convertToEnvelope(
            JsonNode root, TypeReference<EventEnvelope<T>> typeReference) {
        return objectMapper.convertValue(root, typeReference);
    }

    public <T> void consumeByEventType(
            ConsumerRecord<String, String> consumerRecord,
            String expectedEventType,
            TypeReference<EventEnvelope<T>> typeReference,
            BiConsumer<EventEnvelope<T>, String> eventHandler) {
        try {
            // 메시지에서 eventType을 먼저 확인해 예상 이벤트만 처리한다.
            JsonNode root = readTree(consumerRecord.value());
            String actualEventType = extractEventType(root);
            if (!expectedEventType.equals(actualEventType)) {
                log.warn(
                        "Skip unexpected event. topic={}, recordKey={}, expectedEventType={},"
                                + " actualEventType={}",
                        sanitizeForLog(consumerRecord.topic()),
                        sanitizeForLog(consumerRecord.key()),
                        sanitizeForLog(expectedEventType),
                        sanitizeForLog(actualEventType));
                return;
            }

            EventEnvelope<T> envelope = convertToEnvelope(root, typeReference);
            eventHandler.accept(envelope, consumerRecord.key());
        } catch (JsonProcessingException e) {
            log.error(
                    "Failed to parse Kafka payload. topic={}, recordKey={}",
                    sanitizeForLog(consumerRecord.topic()),
                    sanitizeForLog(consumerRecord.key()),
                    e);
        } catch (Exception e) {
            log.error(
                    "Failed to handle Kafka event. topic={}, recordKey={}, expectedEventType={}",
                    sanitizeForLog(consumerRecord.topic()),
                    sanitizeForLog(consumerRecord.key()),
                    sanitizeForLog(expectedEventType),
                    e);
        }
    }

    public String sanitizeForLog(String raw) {
        return logSanitizer.sanitize(raw);
    }
}
