package com.project.domain.usage.infra.messaging;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.project.domain.usage.service.port.UsagePersistEventPublisher;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.usage.UsagePersistPayload;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class UsagePersistKafkaProducer implements UsagePersistEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publish(UsagePersistPayload payload) {
        EventEnvelope<UsagePersistPayload> envelope = EventEnvelope.of("USAGE_PERSIST", payload);

        kafkaTemplate.send("usage-persist", envelope);

        log.info(
                "Published UsagePersist event: {} (Family: {})",
                envelope.eventId(),
                payload.familyId());
    }
}
