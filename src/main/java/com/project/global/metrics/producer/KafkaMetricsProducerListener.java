package com.project.global.metrics.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.stereotype.Component;

import com.project.global.event.dto.EventEnvelope;
import com.project.global.metrics.KafkaMetricTagSanitizer;
import com.project.global.metrics.KafkaMetrics;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class KafkaMetricsProducerListener implements ProducerListener<String, Object> {
    private final KafkaMetrics kafkaMetrics;

    // 성공
    @Override
    public void onSuccess(ProducerRecord<String, Object> producerRecord, RecordMetadata metadata) {
        String eventType = extractEventType(producerRecord.value());
        kafkaMetrics.incrementProducerSendSuccess(producerRecord.topic(), eventType);
    }

    // 실패
    @Override
    public void onError(
            ProducerRecord<String, Object> producerRecord, RecordMetadata metadata, Exception ex) {
        String eventType = extractEventType(producerRecord.value());
        kafkaMetrics.incrementProducerSendError(producerRecord.topic(), eventType);
    }

    private String extractEventType(Object value) {
        if (value instanceof EventEnvelope<?> envelope && envelope.eventType() != null) {
            return KafkaMetricTagSanitizer.normalizeEventType(envelope.eventType());
        }
        return KafkaMetricTagSanitizer.UNKNOWN_EVENT_TYPE;
    }
}
