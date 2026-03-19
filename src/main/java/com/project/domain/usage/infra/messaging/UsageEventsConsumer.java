package com.project.domain.usage.infra.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.dabom.messaging.kafka.contract.KafkaConsumerGroups;
import com.dabom.messaging.kafka.contract.KafkaEventTypes;
import com.dabom.messaging.kafka.contract.KafkaTopics;
import com.dabom.messaging.kafka.event.KafkaEventMessageSupport;
import com.dabom.messaging.kafka.event.consumer.KafkaEventConsumer;
import com.dabom.messaging.kafka.event.dto.EventEnvelope;
import com.dabom.messaging.kafka.event.dto.usage.UsagePayload;
import com.fasterxml.jackson.core.type.TypeReference;
import com.project.common.util.LogSanitizer;
import com.project.domain.usage.service.UsageSyncService;
import com.project.domain.usage.service.helper.UsageEventValidator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class UsageEventsConsumer implements KafkaEventConsumer<UsagePayload> {

    private final KafkaEventMessageSupport kafkaEventMessageSupport;
    private final UsageSyncService usageSyncService;
    private final UsageEventValidator validator;
    private final LogSanitizer logSanitizer;

    @KafkaListener(
            topics = KafkaTopics.USAGE_EVENTS,
            groupId = KafkaConsumerGroups.DABOM_PROCESSOR_USAGE_MAIN,
            concurrency = "${app.kafka.usage-events.concurrency:4}")
    public void consume(ConsumerRecord<String, String> consumerRecord) {
        consume(consumerRecord, kafkaEventMessageSupport);
    }

    @Override
    public String eventType() {
        return KafkaEventTypes.DATA_USAGE;
    }

    @Override
    public TypeReference<EventEnvelope<UsagePayload>> typeReference() {
        return new TypeReference<>() {};
    }

    @Override
    public void handle(EventEnvelope<UsagePayload> envelope, String recordKey) {
        String eventId = envelope.eventId();
        UsagePayload payload = envelope.payload();

        if (!validator.isValid(payload, eventId)) {
            log.warn(
                    "Invalid usage event. Key: {}, EventId: {}",
                    logSanitizer.sanitize(recordKey),
                    logSanitizer.sanitize(eventId));
            throw new IllegalArgumentException(
                    "Invalid usage payload. eventId=" + logSanitizer.sanitize(eventId));
        }

        log.debug(
                "Consumed usage event: {} (Key: {})",
                logSanitizer.sanitize(eventId),
                logSanitizer.sanitize(recordKey));

        usageSyncService.syncUsage(eventId, envelope.timestamp().toString(), payload);
    }
}
