package com.project.domain.usage.infra.messaging;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.usage.UsagePersistPayload;
import com.project.global.kafka.error.KafkaMessageProcessingException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class UsagePersistKafkaProducer implements UsagePersistEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    private static final String TOPIC = "usage-persist";
    private static final String EVENT_TYPE = "USAGE_PERSIST";

    public void publish(UsagePersistPayload payload) {
        EventEnvelope<UsagePersistPayload> envelope = EventEnvelope.of(EVENT_TYPE, payload);

        kafkaTemplate.send(TOPIC, serialize(envelope));

        log.info(
                "Published UsagePersist event: {} (Family: {})",
                envelope.eventId(),
                payload.familyId());
    }

    private String serialize(EventEnvelope<UsagePersistPayload> envelope) {
        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new KafkaMessageProcessingException("Failed to serialize usage persist event", e);
        }
    }
}
