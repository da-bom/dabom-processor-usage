package com.project.domain.usage.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.project.domain.usage.service.helper.UsageEventValidator;
import com.project.global.event.dto.usage.UsagePayload;

class UsageEventValidatorTest {

    private final UsageEventValidator validator = new UsageEventValidator();

    @Test
    @DisplayName("유효한 페이로드는 검증을 통과해야 한다")
    void validPayload() {
        // given
        UsagePayload payload = new UsagePayload(100L, 1L, "com.app.test", 1024L, Map.of());

        // when
        boolean result = validator.isValid(payload, "evt_1");

        // then
        assertThat(result).isTrue();
    }

    @Test
    @DisplayName("필수 값이 누락되면 검증 실패해야 한다")
    void invalidPayload() {
        // given
        UsagePayload payload = new UsagePayload(null, null, null, null, null);

        // when
        boolean result = validator.isValid(payload, "evt_1");

        // then
        assertThat(result).isFalse();
    }

    @Test
    @DisplayName("음수 사용량은 검증 실패해야 한다")
    void negativeBytesUsed() {
        // given
        UsagePayload payload =
                new UsagePayload(
                        100L, 1L, "appId", -1L, // 음수
                        Map.of());

        // when
        boolean result = validator.isValid(payload, "evt_1");

        // then
        assertThat(result).isFalse();
    }
}
