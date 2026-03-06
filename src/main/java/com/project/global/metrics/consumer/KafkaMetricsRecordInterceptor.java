package com.project.global.metrics.consumer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.global.common.TimeConstants;
import com.project.global.metrics.KafkaMetricTagSanitizer;
import com.project.global.metrics.KafkaMetrics;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaMetricsRecordInterceptor implements RecordInterceptor<String, String> {
    private static final String START_NANOS_HEADER = "x-metrics-start-nanos";

    private final KafkaMetrics kafkaMetrics;
    private final ObjectMapper objectMapper;

    @Override
    @Nullable
    public ConsumerRecord<String, String> intercept(
            ConsumerRecord<String, String> consumerRecord, Consumer<String, String> consumer) {
        setStartNanosHeader(consumerRecord);

        String eventName = extractEventType(consumerRecord);
        Instant producedAt = extractProducedAt(consumerRecord);

        kafkaMetrics.recordProducerToConsumerLatency(
                consumerRecord.topic(),
                consumer.groupMetadata().groupId(),
                eventName,
                producedAt,
                Instant.now());

        return consumerRecord;
    }

    @Override
    public void success(
            ConsumerRecord<String, String> consumerRecord, Consumer<String, String> consumer) {
        long started = getStartNanos(consumerRecord);
        String eventName = extractEventType(consumerRecord);
        String group = consumer.groupMetadata().groupId();

        kafkaMetrics.incrementSuccess(consumerRecord.topic(), group, eventName);
        kafkaMetrics.recordProcessingTime(
                consumerRecord.topic(),
                group,
                eventName,
                Duration.ofNanos(System.nanoTime() - started));
    }

    @Override
    public void failure(
            ConsumerRecord<String, String> consumerRecord,
            Exception ex,
            Consumer<String, String> consumer) {
        long started = getStartNanos(consumerRecord);
        String eventName = extractEventType(consumerRecord);
        String group = consumer.groupMetadata().groupId();

        kafkaMetrics.incrementRetryableError(consumerRecord.topic(), group, eventName);
        kafkaMetrics.recordProcessingTime(
                consumerRecord.topic(),
                group,
                eventName,
                Duration.ofNanos(System.nanoTime() - started));
    }

    private String extractEventType(ConsumerRecord<String, String> consumerRecord) {
        String rawValue = consumerRecord.value();
        if (rawValue == null || rawValue.isBlank()) {
            return KafkaMetricTagSanitizer.UNKNOWN_EVENT_TYPE;
        }

        try {
            JsonNode root = objectMapper.readTree(rawValue);
            String rawEventType =
                    root.path("eventType").asText(KafkaMetricTagSanitizer.UNKNOWN_EVENT_TYPE);
            return KafkaMetricTagSanitizer.normalizeEventType(rawEventType);
        } catch (Exception ignored) {
            log.warn("Failed to extract event type from Kafka record. Value: {}", rawValue);
            return KafkaMetricTagSanitizer.UNKNOWN_EVENT_TYPE;
        }
    }

    private void setStartNanosHeader(ConsumerRecord<String, String> consumerRecord) {
        Headers headers = consumerRecord.headers();
        headers.remove(START_NANOS_HEADER);
        headers.add(
                START_NANOS_HEADER,
                Long.toString(System.nanoTime()).getBytes(StandardCharsets.UTF_8));
    }

    private long getStartNanos(ConsumerRecord<String, String> consumerRecord) {
        var header = consumerRecord.headers().lastHeader(START_NANOS_HEADER);
        if (header == null || header.value() == null) {
            return System.nanoTime();
        }

        try {
            return Long.parseLong(new String(header.value(), StandardCharsets.UTF_8));
        } catch (Exception ignored) {
            return System.nanoTime();
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
            log.warn("Failed to parse LocalDateTime");
            return null;
        }
    }
}
