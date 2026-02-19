package com.project.domain.usage.infra.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.project.domain.usage.service.UsagePersistService;
import com.project.global.event.KafkaEventMessageSupport;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class UsagePersistKafkaConsumer {
    private static final String USAGE_PERSIST_EVENT_TYPE = "USAGE_PERSIST";

    private final KafkaEventMessageSupport kafkaEventMessageSupport;
    private final UsagePersistService usagePersistService;

    @KafkaListener(topics = "usage-persist", groupId = "dabom-processor-usage-persistence-group")
    public void consume(ConsumerRecord<String, String> consumerRecord) {
        // eventType 필터링/역직렬화/예외 처리는 공통 유틸에서 수행한다.
        kafkaEventMessageSupport.consumeByEventType(
                consumerRecord,
                USAGE_PERSIST_EVENT_TYPE,
                new TypeReference<>() {},
                usagePersistService::persist);
    }
}
