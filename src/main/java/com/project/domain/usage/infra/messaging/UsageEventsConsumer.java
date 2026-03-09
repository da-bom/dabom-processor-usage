package com.project.domain.usage.infra.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.project.domain.usage.service.UsageSyncService;
import com.project.domain.usage.service.helper.UsageEventValidator;
import com.project.global.event.KafkaEventMessageSupport;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.usage.UsagePayload;
import com.project.global.util.LogSanitizer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class UsageEventsConsumer {

    private static final String GROUP = "dabom-processor-usage-main-group";
    private static final String EVENT_TYPE = "DATA_USAGE";

    private final KafkaEventMessageSupport kafkaEventMessageSupport;
    private final UsageSyncService usageSyncService;
    private final UsageEventValidator validator;
    private final LogSanitizer logSanitizer;

    @KafkaListener(topics = "usage-events", groupId = GROUP)
    public void consume(ConsumerRecord<String, String> consumerRecord) {
        kafkaEventMessageSupport.consumeByEventType(
                consumerRecord, EVENT_TYPE, new TypeReference<>() {}, this::handleUsageEvent);
    }

    private void handleUsageEvent(EventEnvelope<UsagePayload> envelope, String recordKey) {
        String eventId = envelope.eventId();
        UsagePayload payload = envelope.payload();

        if (!validator.isValid(payload, eventId)) {
            log.warn(
                    "Skipping invalid usage event. Key: {}, EventId: {}",
                    logSanitizer.sanitize(recordKey),
                    logSanitizer.sanitize(eventId));
            return;
        }

        log.debug(
                "Consumed usage event: {} (Key: {})",
                logSanitizer.sanitize(eventId),
                logSanitizer.sanitize(recordKey));

        usageSyncService.syncUsage(eventId, envelope.timestamp().toString(), payload);
    }
}
