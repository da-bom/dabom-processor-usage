package com.project.example.infra.messaging;

import java.util.Map;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.project.example.core.Example;
import com.project.example.core.event.ExampleEventPublisher;
import com.project.example.core.event.dto.EventEnvelope;
import com.project.example.core.event.dto.usage.UsagePayload;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * [가이드] Kafka Publisher 구현 예시
 *
 * <p>본 클래스는 이벤트를 발행하는 방법을 보여줍니다. KafkaTemplate을 직접 호출하는 대신, `ExampleEventPublisher` 인터페이스를 구현하여
 * 비즈니스 로직(Core)과 인프라(Infra)를 분리합니다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ExampleKafkaProducer implements ExampleEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * [예시] 데이터 사용량 이벤트 발행
     *
     * <p>1. 실제 전송할 DTO(Payload) 생성 2. EventEnvelope.of()를 사용하여 봉투에 담기 3. KafkaTemplate으로 전송
     */
    @Override
    public void publishExampleCreated(Example example) {
        // 1. Payload 생성 (실제 데이터)
        UsagePayload payload =
                new UsagePayload(
                        "evt_sample_id",
                        100L, // familyId
                        200L, // userId
                        "com.youtube",
                        1024L * 1024L, // 1MB
                        Map.of("network", "WIFI") // metadata
                        );

        // 2. Envelope 생성 (이벤트 타입 지정: "DATA_USAGE")
        EventEnvelope<UsagePayload> envelope = EventEnvelope.of("DATA_USAGE", payload);

        // 3. Kafka 전송
        kafkaTemplate.send("usage-events", envelope);
    }
}
