package com.project.global.metrics;

import java.time.Duration;
import java.time.Instant;

import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class KafkaMetrics {
    private static final String TAG_TOPIC = "topic";
    private static final String TAG_GROUP = "group";
    private static final String TAG_EVENT_TYPE = "eventType";
    private static final String TAG_RESULT = "result";

    private final MeterRegistry meterRegistry;

    public void incrementProducerSendSuccess(String topic, String eventType) {
        String safeEventType = KafkaMetricTagSanitizer.normalizeEventType(eventType);
        meterRegistry
                .counter(
                        "kafka.producer.send.success.count",
                        TAG_TOPIC,
                        topic,
                        TAG_EVENT_TYPE,
                        safeEventType,
                        TAG_RESULT,
                        "success")
                .increment();
    }

    public void incrementProducerSendError(String topic, String eventType) {
        String safeEventType = KafkaMetricTagSanitizer.normalizeEventType(eventType);
        meterRegistry
                .counter(
                        "kafka.producer.send.error.count",
                        TAG_TOPIC,
                        topic,
                        TAG_EVENT_TYPE,
                        safeEventType,
                        TAG_RESULT,
                        "error")
                .increment();
    }

    public void recordProducerSendLatency(String topic, String eventType, Duration duration) {
        String safeEventType = KafkaMetricTagSanitizer.normalizeEventType(eventType);
        Timer.builder("kafka.producer.send.latency")
                .tags(TAG_TOPIC, topic, TAG_EVENT_TYPE, safeEventType)
                .publishPercentileHistogram(true)
                .register(meterRegistry)
                .record(duration);
    }

    public void incrementSuccess(String topic, String group, String eventType) {
        String safeEventType = KafkaMetricTagSanitizer.normalizeEventType(eventType);
        meterRegistry
                .counter(
                        "kafka.consumer.success.count",
                        TAG_TOPIC,
                        topic,
                        TAG_GROUP,
                        group,
                        TAG_EVENT_TYPE,
                        safeEventType)
                .increment();
    }

    public void incrementInvalidEvent(String topic, String group, String eventType) {
        String safeEventType = KafkaMetricTagSanitizer.normalizeEventType(eventType);
        meterRegistry
                .counter(
                        "kafka.consumer.invalid_event.count",
                        TAG_TOPIC,
                        topic,
                        TAG_GROUP,
                        group,
                        TAG_EVENT_TYPE,
                        safeEventType)
                .increment();
    }

    public void incrementRetryableError(String topic, String group, String eventType) {
        String safeEventType = KafkaMetricTagSanitizer.normalizeEventType(eventType);
        meterRegistry
                .counter(
                        "kafka.consumer.retryable_error.count",
                        TAG_TOPIC,
                        topic,
                        TAG_GROUP,
                        group,
                        TAG_EVENT_TYPE,
                        safeEventType)
                .increment();
    }

    public void incrementDlt(String topic, String group, String eventType) {
        String safeEventType = KafkaMetricTagSanitizer.normalizeEventType(eventType);
        meterRegistry
                .counter(
                        "kafka.consumer.dlt.count",
                        TAG_TOPIC,
                        topic,
                        TAG_GROUP,
                        group,
                        TAG_EVENT_TYPE,
                        safeEventType)
                .increment();
    }

    public void incrementDedupHit(String topic, String group, String eventType) {
        String safeEventType = KafkaMetricTagSanitizer.normalizeEventType(eventType);
        meterRegistry
                .counter(
                        "kafka.consumer.dedup_hit.count",
                        TAG_TOPIC,
                        topic,
                        TAG_GROUP,
                        group,
                        TAG_EVENT_TYPE,
                        safeEventType)
                .increment();
    }

    public void recordProcessingTime(
            String topic, String group, String eventType, Duration duration) {
        String safeEventType = KafkaMetricTagSanitizer.normalizeEventType(eventType);
        Timer.builder("kafka.consumer.processing.time")
                .tags(TAG_TOPIC, topic, TAG_GROUP, group, TAG_EVENT_TYPE, safeEventType)
                .publishPercentileHistogram(true)
                .register(meterRegistry)
                .record(duration);
    }

    public void recordProducerToConsumerLatency(
            String topic, String group, String eventType, Instant producedAt, Instant consumedAt) {
        if (producedAt == null || consumedAt == null || consumedAt.isBefore(producedAt)) {
            return;
        }

        Duration latency = Duration.between(producedAt, consumedAt);
        String safeEventType = KafkaMetricTagSanitizer.normalizeEventType(eventType);
        Timer.builder("kafka.consumer.producer_to_consumer.latency")
                .tags(TAG_TOPIC, topic, TAG_GROUP, group, TAG_EVENT_TYPE, safeEventType)
                .publishPercentileHistogram(true)
                .register(meterRegistry)
                .record(latency);
    }
}
