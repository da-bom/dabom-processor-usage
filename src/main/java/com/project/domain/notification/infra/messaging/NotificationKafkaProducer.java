package com.project.domain.notification.infra.messaging;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

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
public class NotificationKafkaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private static final String TOPIC = "notification-events";

    public void publish(NotificationPayload payload) {

        var extraction =
                switch (payload) {
                    case QuotaUpdatedPayload p -> new Extraction("QUOTA_UPDATED", p.familyId());
                    case CustomerBlockedPayload p ->
                            new Extraction("CUSTOMER_BLOCKED", p.familyId());
                    case ThresholdAlertPayload p -> new Extraction("THRESHOLD_ALERT", p.familyId());
                };

        EventEnvelope<NotificationPayload> envelope =
                EventEnvelope.of("NOTIFICATION", extraction.subType(), payload);

        kafkaTemplate.send(TOPIC, envelope);

        log.info(
                "Published Notification event: {} (Type: {}, Family: {})",
                envelope.eventId(),
                extraction.subType(),
                extraction.familyId());
    }

    // 내부 처리를 위한 임시 Record
    private record Extraction(String subType, Long familyId) {}
}
