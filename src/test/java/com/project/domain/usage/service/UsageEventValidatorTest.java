package com.project.domain.usage.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.dabom.messaging.kafka.event.dto.usage.UsagePayload;
import com.project.domain.usage.service.helper.UsageEventValidator;

class UsageEventValidatorTest {

    private final UsageEventValidator validator = new UsageEventValidator();

    @Test
    @DisplayName("유효한 usage payload는 검증을 통과한다")
    void validPayload() {
        UsagePayload payload = new UsagePayload(100L, 1L, "com.app.test", 1024L, Map.of());

        boolean result = validator.isValid(payload, "evt_1");

        assertThat(result).isTrue();
    }

    @Test
    @DisplayName("필수 값이 없으면 검증에 실패한다")
    void invalidPayload() {
        UsagePayload payload = new UsagePayload(null, null, null, null, null);

        boolean result = validator.isValid(payload, "evt_1");

        assertThat(result).isFalse();
    }

    @Test
    @DisplayName("음수 사용량이면 검증에 실패한다")
    void negativeBytesUsed() {
        UsagePayload payload = new UsagePayload(100L, 1L, "appId", -1L, Map.of());

        boolean result = validator.isValid(payload, "evt_1");

        assertThat(result).isFalse();
    }

    @Test
    @DisplayName("0 사용량이면 검증에 실패한다")
    void zeroBytesUsed() {
        UsagePayload payload = new UsagePayload(100L, 1L, "appId", 0L, Map.of());

        boolean result = validator.isValid(payload, "evt_1");

        assertThat(result).isFalse();
    }
}
