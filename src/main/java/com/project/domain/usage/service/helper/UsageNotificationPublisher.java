package com.project.domain.usage.service.helper;

import java.util.concurrent.CompletableFuture;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import com.dabom.messaging.kafka.contract.KafkaTopics;
import com.dabom.messaging.kafka.event.KafkaEventMessageSupport;
import com.dabom.messaging.kafka.event.dto.EventEnvelope;
import com.dabom.messaging.kafka.event.dto.notification.NotificationEventSupport;
import com.dabom.messaging.kafka.event.dto.notification.NotificationPayload;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class UsageNotificationPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaEventMessageSupport kafkaEventMessageSupport;

    // notification을 비동기로 발행하고 broker ack future를 반환한다.
    public CompletableFuture<SendResult<String, String>> publishAsync(NotificationPayload payload) {
        EventEnvelope<NotificationPayload> envelope = NotificationEventSupport.toEnvelope(payload);
        String serialized = kafkaEventMessageSupport.serialize(envelope);
        return kafkaTemplate.send(
                KafkaTopics.NOTIFICATION, String.valueOf(payload.customerId()), serialized);
    }
}
