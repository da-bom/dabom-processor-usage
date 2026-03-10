package com.project.domain.usage.infra.messaging;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import com.project.global.event.KafkaEventMessageSupport;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.usage.UsageRealtimePayload;
import com.project.global.kafka.error.KafkaMessageProcessingException;

@ExtendWith(MockitoExtension.class)
class UsageRealtimeKafkaProducerTest {

    @InjectMocks private UsageRealtimeKafkaProducer producer;

    @Mock private KafkaTemplate<String, String> kafkaTemplate;
    @Mock private KafkaEventMessageSupport kafkaEventMessageSupport;

    @Test
    @DisplayName("publish serializes the envelope and sends it to the usage-realtime topic")
    void publish_SendsSerializedEnvelope() {
        UsageRealtimePayload payload =
                new UsageRealtimePayload(100L, 1L, 500L, 1000L, 500L, 50.0, 300L, 30.0, 1000L);

        given(kafkaEventMessageSupport.serialize(any(EventEnvelope.class)))
                .willReturn("serialized");

        producer.publish(payload);

        verify(kafkaEventMessageSupport).serialize(any(EventEnvelope.class));
        verify(kafkaTemplate).send("usage-realtime", "serialized");
    }

    @Test
    @DisplayName("publish rethrows serialization exceptions and does not send to Kafka")
    void publish_WhenSerializationFails_ThrowsException() {
        UsageRealtimePayload payload =
                new UsageRealtimePayload(100L, 1L, 500L, 1000L, 500L, 50.0, 300L, 30.0, 1000L);

        given(kafkaEventMessageSupport.serialize(any(EventEnvelope.class)))
                .willThrow(
                        new KafkaMessageProcessingException(
                                "Failed to serialize event", new RuntimeException("serialize")));

        assertThrows(KafkaMessageProcessingException.class, () -> producer.publish(payload));

        verify(kafkaTemplate, never()).send(any(String.class), any(String.class));
    }
}
