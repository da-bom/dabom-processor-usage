package com.project.domain.example.infra.messaging;

import java.util.Map;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.project.domain.example.entity.Example;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.usage.UsagePayload;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class ExampleKafkaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publishExampleCreated(Example example) {
        UsagePayload payload =
                new UsagePayload(
                        "evt_sample_id",
                        100L,
                        200L,
                        "com.youtube",
                        1024L * 1024L,
                        Map.of("network", "WIFI"));

        EventEnvelope<UsagePayload> envelope = EventEnvelope.of("DATA_USAGE", payload);

        kafkaTemplate.send("usage-events", envelope);
    }
}
