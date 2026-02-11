package com.project.example.infra.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.example.core.event.dto.EventEnvelope;
import com.project.example.core.event.dto.usage.UsagePayload;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * [가이드] Kafka Consumer 구현 예시
 *
 * <p>본 클래스는 Kafka 메시지를 수신하여 비즈니스 로직을 처리하는 방법을 보여줍니다. 모든 이벤트는
 * [EventEnvelope](src/main/java/com/project/example/core/event/dto/EventEnvelope.java:14:0-45:1)로
 * 감싸져 있으므로, `TypeReference`를 사용하여 적절한 DTO 타입으로 역직렬화해야 합니다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ExampleKafkaConsumer {

    private final ObjectMapper objectMapper;

    // private final UsageService usageService; // 실제 로직을 처리할 서비스 주입

    /**
     * [예시 ] 데이터 사용량 이벤트 수신
     *
     * @param record Kafka ConsumerRecord (String key, Object value)
     */
    @KafkaListener(topics = "usage-events", groupId = "usage-group")
    public void consume(ConsumerRecord<String, String> record) {
        try {
            // 1. Envelope<T> 형태로 역직렬화 (T = UsagePayload)
            // 제네릭 타입을 명시하기 위해 TypeReference를 필수적으로 사용
            EventEnvelope<UsagePayload> envelope =
                    objectMapper.readValue(
                            record.value(), new TypeReference<EventEnvelope<UsagePayload>>() {});

            // 2. Payload 추출
            UsagePayload payload = envelope.payload();

            // 3. 비즈니스 로직 호출
            // usageService.processUsage(payload);
            log.info(
                    "EventId:{}, FamilyId:{}, AppId:{}, BytesUsed:{}",
                    payload.eventId(),
                    payload.familyId(),
                    payload.appId(),
                    payload.bytesUsed());

        } catch (JsonProcessingException e) {
            log.error("JSON 파싱 실패", e);
        }
    }
}
