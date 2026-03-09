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
import com.project.global.kafka.error.KafkaMessageProcessingException;
import com.project.global.util.LogSanitizer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class UsageEventsConsumer {

    private static final String GROUP = "dabom-processor-usage-main-group";

    private final ObjectMapper objectMapper;
    private final UsageSyncService usageSyncService;
    private final UsageEventValidator validator;
    private final LogSanitizer logSanitizer;

    @KafkaListener(topics = "usage-events", groupId = GROUP)
    public void consume(ConsumerRecord<String, String> consumerRecord) {
        try {
            EventEnvelope<UsagePayload> envelope =
                    objectMapper.readValue(
                            consumerRecord.value(),
                            new TypeReference<EventEnvelope<UsagePayload>>() {});

            String eventId = envelope.eventId();
            UsagePayload payload = envelope.payload();

            if (!validator.isValid(payload, eventId)) {
                log.warn(
                        "Skipping invalid usage event. Key: {}, EventId: {}",
                        logSanitizer.sanitize(consumerRecord.key()),
                        logSanitizer.sanitize(eventId));
                return;
            }

            log.debug(
                    "Consumed usage event: {} (Key: {})",
                    logSanitizer.sanitize(eventId),
                    logSanitizer.sanitize(consumerRecord.key()));

            usageSyncService.syncUsage(eventId, envelope.timestamp().toString(), payload);
        } catch (Exception e) {
            log.error(
                    "Failed to process usage event [Key: {}]: {}",
                    logSanitizer.sanitize(consumerRecord.key()),
                    logSanitizer.sanitize(e.getMessage()),
                    e);
            throw new KafkaMessageProcessingException("Failed to process usage event", e);
        }
    }
}
