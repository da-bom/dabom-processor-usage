package com.project.example.core.event;

import com.project.example.core.Example;

/**
 * Example 이벤트 발행 (Port Interface) - Domain Layer에서 이벤트 발행 시 사용하는 인터페이스 - Infra Layer(Kafka)와 결합도 제거
 */
public interface ExampleEventPublisher {
    void publishExampleCreated(Example example);
}
