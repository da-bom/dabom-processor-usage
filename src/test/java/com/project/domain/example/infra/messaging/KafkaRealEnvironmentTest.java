package com.project.domain.example.infra.messaging;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import com.project.domain.example.entity.Example;

@SpringBootTest
@DirtiesContext
@ActiveProfiles("test")
@EmbeddedKafka(
        partitions = 1,
        topics = {"usage-events"},
        brokerProperties = {"listeners=PLAINTEXT://localhost:0", "port=0"})
class KafkaRealEnvironmentTest {

    @Autowired private ExampleKafkaProducer producer;

    @Autowired private KafkaListenerEndpointRegistry registry;

    @MockitoSpyBean private ExampleKafkaConsumer consumer;

    @Test
    @DisplayName("EmbeddedKafka를 사용하여 이벤트를 보내고 받는지 확인한다")
    void testEmbeddedKafkaFlow() {
        // Consumer 파티션 할당 대기
        for (MessageListenerContainer container : registry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, 1);
        }

        // given
        Example example =
                Example.builder()
                        .exampleId(777L)
                        .exampleName("Embedded Kafka Test")
                        .exampleContent("Using spring-kafka-test")
                        .build();

        // when
        producer.publishExampleCreated(example);

        // then
        verify(consumer, timeout(10000).times(1)).consume(any());
    }
}
