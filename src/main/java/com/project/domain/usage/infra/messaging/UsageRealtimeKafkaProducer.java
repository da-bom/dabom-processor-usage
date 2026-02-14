package com.project.domain.usage.infra.messaging;

import com.project.domain.usage.service.port.UsageRealtimeEventPublisher;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.usage.UsageRealtimePayload;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class UsageRealtimeKafkaProducer implements UsageRealtimeEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String TOPIC = "usage-realtime";
    private static final String EVENT_TYPE = "USAGE_REALTIME";

    public void publish(UsageRealtimePayload payload) {
        EventEnvelope<UsageRealtimePayload> envelope = EventEnvelope.of(EVENT_TYPE, payload);

        kafkaTemplate.send(TOPIC, envelope);

        log.debug(
                "Published UsageRealtime event: {} (Family: {})",
                envelope.eventId(),
                payload.familyId());
    }
}
