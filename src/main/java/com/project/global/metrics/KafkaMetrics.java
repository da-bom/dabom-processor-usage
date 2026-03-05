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

    private final MeterRegistry meterRegistry;

    /** 토픽/이벤트 타입 기준으로 Producer 발행 성공 카운트를 증가시킨다. */
    public void incrementProducerSendSuccess(String topic, String eventType) {
        meterRegistry
                .counter(
                        "kafka.producer.send.success.count",
                        "topic",
                        topic,
                        "eventType",
                        eventType,
                        "result",
                        "success")
                .increment();
    }

    /** 토픽/이벤트 타입 기준으로 Producer 발행 실패 카운트를 증가시킨다. */
    public void incrementProducerSendError(String topic, String eventType) {
        meterRegistry
                .counter(
                        "kafka.producer.send.error.count",
                        "topic",
                        topic,
                        "eventType",
                        eventType,
                        "result",
                        "error")
                .increment();
    }

    /** Producer 발행 지연 시간을 기록한다. */
    public void recordProducerSendLatency(String topic, String eventType, Duration duration) {
        Timer.builder("kafka.producer.send.latency")
                .tags("topic", topic, "eventType", eventType)
                .publishPercentileHistogram(true)
                .register(meterRegistry)
                .record(duration);
    }

    /** Consumer 처리 성공 카운트를 증가시킨다. */
    public void incrementSuccess(String topic, String group, String eventType) {
        meterRegistry
                .counter(
                        "kafka.consumer.success.count",
                        "topic",
                        topic,
                        "group",
                        group,
                        "eventType",
                        eventType)
                .increment();
    }

    /** 유효성/파싱 실패(비재시도 대상) 카운트를 증가시킨다. */
    public void incrementInvalidEvent(String topic, String group, String eventType) {
        meterRegistry
                .counter(
                        "kafka.consumer.invalid_event.count",
                        "topic",
                        topic,
                        "group",
                        group,
                        "eventType",
                        eventType)
                .increment();
    }

    /** 재시도 가능한 오류 카운트를 증가시킨다. */
    public void incrementRetryableError(String topic, String group, String eventType) {
        meterRegistry
                .counter(
                        "kafka.consumer.retryable_error.count",
                        "topic",
                        topic,
                        "group",
                        group,
                        "eventType",
                        eventType)
                .increment();
    }

    /** 재시도 소진 후 DLT 전송 카운트를 증가시킨다. */
    public void incrementDlt(String topic, String group, String eventType) {
        meterRegistry
                .counter(
                        "kafka.consumer.dlt.count",
                        "topic",
                        topic,
                        "group",
                        group,
                        "eventType",
                        eventType)
                .increment();
    }

    /** 중복 이벤트 스킵 시 dedup hit 카운트를 증가시킨다. */
    public void incrementDedupHit(String topic, String group, String eventType) {
        meterRegistry
                .counter(
                        "kafka.consumer.dedup_hit.count",
                        "topic",
                        topic,
                        "group",
                        group,
                        "eventType",
                        eventType)
                .increment();
    }

    /** Consumer 처리 시간(수신 시작 -> 비즈니스 처리 종료)을 기록한다. */
    public void recordProcessingTime(
            String topic, String group, String eventType, Duration duration) {
        Timer.builder("kafka.consumer.processing.time")
                .tags("topic", topic, "group", group, "eventType", eventType)
                .publishPercentileHistogram(true)
                .register(meterRegistry)
                .record(duration);
    }

    /** 종단 지연 시간(Producer timestamp -> Consumer 수신 시각)을 기록한다. */
    public void recordProducerToConsumerLatency(
            String topic, String group, String eventType, Instant producedAt, Instant consumedAt) {
        if (producedAt == null || consumedAt == null || consumedAt.isBefore(producedAt)) {
            return;
        }

        Duration latency = Duration.between(producedAt, consumedAt);
        Timer.builder("kafka.consumer.producer_to_consumer.latency")
                .tags("topic", topic, "group", group, "eventType", eventType)
                .publishPercentileHistogram(true)
                .register(meterRegistry)
                .record(latency);
    }
}
