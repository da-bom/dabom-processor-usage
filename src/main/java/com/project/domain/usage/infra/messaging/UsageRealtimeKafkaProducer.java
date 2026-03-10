package com.project.domain.usage.infra.messaging;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.project.global.event.KafkaEventMessageSupport;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.usage.UsageRealtimePayload;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class UsageRealtimeKafkaProducer implements UsageRealtimeEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaEventMessageSupport kafkaEventMessageSupport;

    private static final String TOPIC = "usage-realtime";
    private static final String EVENT_TYPE = "USAGE_REALTIME";

    public void publish(UsageRealtimePayload payload) {
        EventEnvelope<UsageRealtimePayload> envelope = EventEnvelope.of(EVENT_TYPE, payload);

        kafkaTemplate.send(TOPIC, kafkaEventMessageSupport.serialize(envelope));

        log.info(
                "Published UsageRealtime event: {} (Family: {})",
                envelope.eventId(),
                payload.familyId());
    }
}
