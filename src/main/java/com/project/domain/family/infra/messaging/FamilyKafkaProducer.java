package com.project.domain.family.infra.messaging;

import java.util.Map;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.project.domain.family.entity.Family;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.usage.UsagePayload;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class FamilyKafkaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publishFamilyCreated(Family family) {
        UsagePayload payload =
                new UsagePayload(
                        "family_created_" + family.getId(),
                        family.getId(),
                        family.getCreatedById(),
                        "family.created",
                        0L,
                        Map.of("familyName", family.getName()));

        EventEnvelope<UsagePayload> envelope = EventEnvelope.of("FAMILY_CREATED", payload);

        kafkaTemplate.send("family-events", envelope);
    }
}
