package com.project.domain.usage.infra.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.domain.usage.service.UsageSyncService;
import com.project.domain.usage.service.helper.UsageEventValidator;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.usage.UsagePayload;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class UsageEventsConsumer {

    private final ObjectMapper objectMapper;
    private final UsageSyncService usageSyncService;
    private final UsageEventValidator validator;

    @KafkaListener(topics = "usage-events", groupId = "dabom-processor-usage-main-group")
    public void consume(ConsumerRecord<String, String> consumerRecord) {
        try {
            // JSON 역직렬화
            EventEnvelope<UsagePayload> envelope =
                    objectMapper.readValue(
                            consumerRecord.value(),
                            new TypeReference<EventEnvelope<UsagePayload>>() {});

            String eventId = envelope.eventId();
            UsagePayload payload = envelope.payload();

            // 이벤트 검증
            if (!validator.isValid(payload, eventId)) {
                log.warn(
                        "Skipping invalid usage event. Key: {}, EventId: {}",
                        consumerRecord.key(),
                        eventId);
                return;
            }

            log.debug("Consumed usage event: {} (Key: {})", eventId, consumerRecord.key());

            // 비즈니스 로직 위임
            usageSyncService.syncUsage(eventId, envelope.timestamp().toString(), payload);
        } catch (Exception e) {
            // 에러 발생 시 로그만 남기고 넘김
            log.error(
                    "Failed to process usage event [Key: {}]: {}",
                    consumerRecord.key(),
                    e.getMessage(),
                    e);
        }
    }
}
