package com.project.global.event;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.global.event.dto.EventEnvelope;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class KafkaEventMessageSupport {
    private static final String EVENT_TYPE_FIELD = "eventType";
    private static final int MAX_LOG_VALUE_LENGTH = 128;

    private final ObjectMapper objectMapper;

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

    public String sanitizeForLog(String raw) {
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
