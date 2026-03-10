package com.project.domain.usage.infra.messaging;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.project.global.event.KafkaEventMessageSupport;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.notification.CustomerBlockedPayload;
import com.project.global.event.dto.notification.NotificationPayload;
import com.project.global.event.dto.notification.QuotaUpdatedPayload;
import com.project.global.event.dto.notification.ThresholdAlertPayload;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationKafkaProducer implements NotificationEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaEventMessageSupport kafkaEventMessageSupport;

    private static final String TOPIC = "notification-events";
    private static final String EVENT_TYPE = "NOTIFICATION";

    public void publish(NotificationPayload payload) {

        String subType =
                switch (payload) {
                    case QuotaUpdatedPayload p -> "QUOTA_UPDATED";
                    case CustomerBlockedPayload p -> "CUSTOMER_BLOCKED";
                    case ThresholdAlertPayload p -> "THRESHOLD_ALERT";
                };

        EventEnvelope<NotificationPayload> envelope =
                EventEnvelope.of(EVENT_TYPE, subType, payload);

        kafkaTemplate.send(TOPIC, kafkaEventMessageSupport.serialize(envelope));

        log.info("Published Notification event: {} (subType: {})", envelope.eventId(), subType);
    }
}
