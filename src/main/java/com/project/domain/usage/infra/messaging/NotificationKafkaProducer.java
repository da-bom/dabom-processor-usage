package com.project.domain.usage.infra.messaging;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.notification.CustomerBlockedPayload;
import com.project.global.event.dto.notification.NotificationPayload;
import com.project.global.event.dto.notification.QuotaUpdatedPayload;
import com.project.global.event.dto.notification.ThresholdAlertPayload;
import com.project.global.kafka.error.KafkaMessageProcessingException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationKafkaProducer implements NotificationEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private static final String TOPIC = "notification-events";

    public void publish(NotificationPayload payload) {

        String subType =
                switch (payload) {
                    case QuotaUpdatedPayload p -> "QUOTA_UPDATED";
                    case CustomerBlockedPayload p -> "CUSTOMER_BLOCKED";
                    case ThresholdAlertPayload p -> "THRESHOLD_ALERT";
                };

        EventEnvelope<NotificationPayload> envelope =
                EventEnvelope.of("NOTIFICATION", subType, payload);

        kafkaTemplate.send(TOPIC, serialize(envelope));

        log.info("Published Notification event: {} (Type: {})", envelope.eventId(), subType);
    }

    private String serialize(EventEnvelope<NotificationPayload> envelope) {
        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new KafkaMessageProcessingException("Failed to serialize notification event", e);
        }
    }
}
