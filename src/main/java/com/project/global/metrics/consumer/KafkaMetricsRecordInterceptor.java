package com.project.global.metrics.consumer;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.global.common.TimeConstants;
import com.project.global.metrics.KafkaMetrics;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class KafkaMetricsRecordInterceptor implements RecordInterceptor<String, String> {
    private final KafkaMetrics kafkaMetrics;
    private final ObjectMapper objectMapper;
    private final ConcurrentMap<String, Long> start = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> eventType = new ConcurrentHashMap<>();

    @Override
    public ConsumerRecord<String, String> intercept(
            ConsumerRecord<String, String> consumerRecord, Consumer<String, String> consumer) {
        start.put(key(consumerRecord), System.nanoTime());

        String eventName = extractEventType(consumerRecord);
        eventType.put(key(consumerRecord), eventName);

        Instant producedAt = extractProducedAt(consumerRecord);
        kafkaMetrics.recordProducerToConsumerLatency(
                consumerRecord.topic(),
                consumer.groupMetadata().groupId(),
                eventName,
                producedAt,
                Instant.now());

        return consumerRecord;
    }

    // 성공했을 때 동작
    @Override
    public void success(
            ConsumerRecord<String, String> consumerRecord, Consumer<String, String> consumer) {
        long started = start.getOrDefault(key(consumerRecord), System.nanoTime());
        String eventName = eventType.getOrDefault(key(consumerRecord), "UNKNOWN");
        kafkaMetrics.incrementSuccess(
                consumerRecord.topic(), consumer.groupMetadata().groupId(), eventName);
        kafkaMetrics.recordProcessingTime(
                consumerRecord.topic(),
                consumer.groupMetadata().groupId(),
                eventName,
                Duration.ofNanos(System.nanoTime() - started));
        start.remove(key(consumerRecord));
        eventType.remove(key(consumerRecord));
    }

    // 실패했을 때 동작
    @Override
    public void failure(
            ConsumerRecord<String, String> consumerRecord,
            Exception ex,
            Consumer<String, String> consumer) {
        long started = start.getOrDefault(key(consumerRecord), System.nanoTime());
        String eventName = eventType.getOrDefault(key(consumerRecord), "UNKNOWN");
        kafkaMetrics.incrementRetryableError(
                consumerRecord.topic(), consumer.groupMetadata().groupId(), eventName);
        kafkaMetrics.recordProcessingTime(
                consumerRecord.topic(),
                consumer.groupMetadata().groupId(),
                eventName,
                Duration.ofNanos(System.nanoTime() - started));
        start.remove(key(consumerRecord));
        eventType.remove(key(consumerRecord));
    }

    // 키 생성
    private String key(ConsumerRecord<String, String> consumerRecord) {
        return consumerRecord.topic()
                + "-"
                + consumerRecord.partition()
                + "-"
                + consumerRecord.offset();
    }

    // 이벤트 타입 추출
    private String extractEventType(ConsumerRecord<String, String> consumerRecord) {
        String rawValue = consumerRecord.value();
        if (rawValue == null || rawValue.isBlank()) {
            return "UNKNOWN";
        }
        try {
            JsonNode root = objectMapper.readTree(rawValue);
            return root.path("eventType").asText("UNKNOWN");
        } catch (Exception ignored) {
            return "UNKNOWN";
        }
    }

    private Instant extractProducedAt(ConsumerRecord<String, String> consumerRecord) {
        try {
            String ts = objectMapper.readTree(consumerRecord.value()).path("timestamp").asText();
            if (ts == null || ts.isBlank()) {
                return null;
            }
            return LocalDateTime.parse(ts).atZone(TimeConstants.ASIA_SEOUL).toInstant();
        } catch (Exception ignored) {
            return null;
        }
    }
}
