package com.project.domain.usage.infra.messaging;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.domain.usage.service.UsageEventValidator;
import com.project.domain.usage.service.UsageSyncService;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.usage.UsagePayload;

@ExtendWith(MockitoExtension.class)
class UsageEventsConsumerTest {

    @InjectMocks private UsageEventsConsumer consumer;

    @Mock private ObjectMapper objectMapper;

    @Mock private UsageSyncService usageSyncService;

    @Mock private UsageEventValidator validator;

    @Test
    @DisplayName("유효한 메시지는 검증 후 서비스를 호출해야 한다")
    void consume_ValidMessage() throws JsonProcessingException {
        // given
        String json = "{\"eventId\":\"evt_1\", ...}";
        ConsumerRecord<String, String> consumerRecord =
                new ConsumerRecord<>("topic", 0, 0L, "key", json);

        EventEnvelope<UsagePayload> envelope =
                EventEnvelope.of("USAGE", new UsagePayload(100L, 1L, "app", 100L, Map.of()));

        // Mocking
        given(objectMapper.readValue(eq(json), any(TypeReference.class))).willReturn(envelope);

        given(validator.isValid(any(UsagePayload.class), anyString())).willReturn(true);

        // when
        consumer.consume(consumerRecord);

        // then
        verify(usageSyncService, times(1))
                .syncUsage(
                        eq(envelope.eventId()),
                        anyString(), // timestamp string
                        any(UsagePayload.class));
    }

    @Test
    @DisplayName("Validator가 실패하면 서비스를 호출하지 않아야 한다")
    void consume_InvalidPayload() throws JsonProcessingException {
        // given
        String json = "{\"eventId\":\"evt_invalid\", ...}";
        ConsumerRecord<String, String> consumerRecord =
                new ConsumerRecord<>("topic", 0, 0L, "key", json);

        EventEnvelope<UsagePayload> envelope =
                EventEnvelope.of("USAGE", new UsagePayload(null, null, null, null, null));

        given(objectMapper.readValue(eq(json), any(TypeReference.class))).willReturn(envelope);

        // Validator returns false
        given(validator.isValid(any(UsagePayload.class), anyString())).willReturn(false);

        // when
        consumer.consume(consumerRecord);

        // then
        verify(usageSyncService, never()).syncUsage(any(), any(), any());
    }

    @Test
    @DisplayName("JSON 역직렬화 실패 시 예외를 잡고 서비스를 호출하지 않아야 한다")
    void consume_DeserializationError() throws JsonProcessingException {
        // given
        String invalidJson = "invalid-json";
        ConsumerRecord<String, String> consumerRecord =
                new ConsumerRecord<>("topic", 0, 0L, "key", invalidJson);

        given(objectMapper.readValue(eq(invalidJson), any(TypeReference.class)))
                .willThrow(new RuntimeException("JSON Error"));

        // when
        consumer.consume(consumerRecord);

        // then
        verify(usageSyncService, never()).syncUsage(any(), any(), any());
    }
}
