package com.project.domain.usage.infra.messaging;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.usage.UsageRealtimePayload;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class UsageRealtimeKafkaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publish(UsageRealtimePayload payload) {
        EventEnvelope<UsageRealtimePayload> envelope = EventEnvelope.of("USAGE_REALTIME", payload);

        kafkaTemplate.send("usage-realtime", envelope);

        log.debug(
                "Published UsageRealtime event: {} (Family: {})",
                envelope.eventId(),
                payload.familyId());
    }
}
