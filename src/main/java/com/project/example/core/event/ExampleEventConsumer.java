package com.project.example.core.event;

import org.springframework.stereotype.Component;

import com.project.example.core.event.dto.ExampleCreatedEvent;

import lombok.extern.slf4j.Slf4j;

/**
 * Example 이벤트 처리 핸들러 (Business Logic) - Kafka Consumer 등에서 수신한 이벤트를 실질적으로 처리 - 비즈니스 로직 수행 (알림, 타
 * 도메인 호출 등)
 */
@Slf4j
@Component
public class ExampleEventConsumer {

    public void handleExampleCreated(ExampleCreatedEvent event) {
        log.info("Example created event consumed: {}", event);
        // 비즈니스 로직 수행 (예: 알림 발송, 타 도메인 호출 등)
    }
}
