package com.project.example.infra.messaging;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import com.project.example.core.Example;
import com.project.example.core.event.ExampleEventPublisher;

@SpringBootTest
class KafkaRealEnvironmentTest {

    @Autowired private ExampleEventPublisher publisher;

    @MockitoSpyBean private ExampleKafkaConsumer consumer;

    @Test
    @DisplayName("실제 Docker Kafka에 이벤트를 보내고 받는지 확인한다")
    void testRealKafkaFlow() {
        // given
        Example example =
                Example.withId(777L, "Real Docker Test 3.4", "Connecting to localhost:9092");

        // when
        System.out.println("[TEST] Sending event to Real Kafka...");
        publisher.publishExampleCreated(example);

        // then
        // 실제 네트워크 통신이라 약간의 딜레이가 있을 수 있으니 10초 대기
        verify(consumer, timeout(10000).times(1)).consume(any());

        System.out.println("[TEST] Successfully consumed event from Real Kafka!");
    }
}
