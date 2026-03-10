package com.project.domain.usage.infra.messaging;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.project.domain.usage.service.UsageSyncService;
import com.project.domain.usage.service.helper.UsageEventValidator;
import com.project.global.event.KafkaEventMessageSupport;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.usage.UsagePayload;
import com.project.global.kafka.error.KafkaMessageProcessingException;
import com.project.global.util.LogSanitizer;

@ExtendWith(MockitoExtension.class)
class UsageEventsConsumerTest {

    @InjectMocks private UsageEventsConsumer consumer;

    @Mock private KafkaEventMessageSupport kafkaEventMessageSupport;
    @Mock private UsageSyncService usageSyncService;
    @Mock private UsageEventValidator validator;
    @Mock private LogSanitizer logSanitizer;

    @BeforeEach
    void setUp() {
        lenient()
                .when(logSanitizer.sanitize(nullable(String.class)))
                .thenAnswer(
                        invocation -> {
                            String raw = invocation.getArgument(0);
                            return raw == null ? "null" : raw;
                        });
    }

    @Test
    @DisplayName("consume calls sync service for a valid usage event")
    void consume_ValidMessage() {
        String json = "{\"eventId\":\"evt_1\", ...}";
        ConsumerRecord<String, String> consumerRecord =
                new ConsumerRecord<>("topic", 0, 0L, "key", json);

        EventEnvelope<UsagePayload> envelope =
                EventEnvelope.of("DATA_USAGE", new UsagePayload(100L, 1L, "app", 100L, Map.of()));

        doAnswer(
                        invocation -> {
                            @SuppressWarnings("unchecked")
                            BiConsumer<EventEnvelope<UsagePayload>, String> handler =
                                    invocation.getArgument(3);
                            handler.accept(envelope, consumerRecord.key());
                            return null;
                        })
                .when(kafkaEventMessageSupport)
                .consumeByEventType(eq(consumerRecord), eq("DATA_USAGE"), any(), any());

        given(validator.isValid(any(UsagePayload.class), anyString())).willReturn(true);

        consumer.consume(consumerRecord);

        verify(usageSyncService, times(1))
                .syncUsage(eq(envelope.eventId()), anyString(), eq(envelope.payload()));
    }

    @Test
    @DisplayName("consume skips sync when the payload is invalid")
    void consume_InvalidPayload() {
        String json = "{\"eventId\":\"evt_invalid\", ...}";
        ConsumerRecord<String, String> consumerRecord =
                new ConsumerRecord<>("topic", 0, 0L, "key", json);

        EventEnvelope<UsagePayload> envelope =
                EventEnvelope.of("DATA_USAGE", new UsagePayload(null, null, null, null, null));

        doAnswer(
                        invocation -> {
                            @SuppressWarnings("unchecked")
                            BiConsumer<EventEnvelope<UsagePayload>, String> handler =
                                    invocation.getArgument(3);
                            handler.accept(envelope, consumerRecord.key());
                            return null;
                        })
                .when(kafkaEventMessageSupport)
                .consumeByEventType(eq(consumerRecord), eq("DATA_USAGE"), any(), any());

        given(validator.isValid(any(UsagePayload.class), anyString())).willReturn(false);

        consumer.consume(consumerRecord);

        verify(usageSyncService, never()).syncUsage(any(), any(), any());
    }

    @Test
    @DisplayName("consume rethrows processing exceptions from support")
    void consume_DeserializationError() {
        String invalidJson = "invalid-json";
        ConsumerRecord<String, String> consumerRecord =
                new ConsumerRecord<>("topic", 0, 0L, "key", invalidJson);

        doThrow(
                        new KafkaMessageProcessingException(
                                "JSON Error", new RuntimeException("JSON Error")))
                .when(kafkaEventMessageSupport)
                .consumeByEventType(eq(consumerRecord), eq("DATA_USAGE"), any(), any());

        assertThrows(KafkaMessageProcessingException.class, () -> consumer.consume(consumerRecord));

        verify(usageSyncService, never()).syncUsage(any(), any(), any());
    }
}
