package com.project.domain.policy.service.helper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.project.domain.policy.helper.PolicyConstraintMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.domain.policy.enums.PolicyType;

class PolicyConstraintMapperTest {
    private final PolicyConstraintMapper mapper = new PolicyConstraintMapper(new ObjectMapper());

    @Test
    @DisplayName("MONTHLY_LIMIT JSON은 Redis 월 제한 값으로 정규화한다")
    void monthlyLimitJson_normalizeToLongString() {
        String normalized =
                mapper.normalizeValue(PolicyType.MONTHLY_LIMIT, "{\"limitBytes\": 6591920494}");

        assertThat(normalized).isEqualTo("6591920494");
    }

    @Test
    @DisplayName("TIME_BLOCK JSON은 HHMM-HHMM으로 정규화한다")
    void timeBlockJson_normalizeToRange() {
        String normalized =
                mapper.normalizeValue(
                        PolicyType.TIME_BLOCK,
                        "{\"start\":\"23:00\",\"end\":\"08:00\",\"timezone\":\"Asia/Seoul\"}");

        assertThat(normalized).isEqualTo("2300-0800");
    }

    @Test
    @DisplayName("MANUAL_BLOCK JSON은 BLOCK:ACCESS 활성값으로 정규화한다")
    void manualBlockJson_normalizeToOne() {
        String normalized =
                mapper.normalizeValue(PolicyType.MANUAL_BLOCK, "{\"reason\":\"MANUAL\"}");

        assertThat(normalized).isEqualTo("1");
    }

    @Test
    @DisplayName("제약 해제(newValue null)는 삭제로 처리한다")
    void nullValue_normalizeToNull() {
        String normalized = mapper.normalizeValue(PolicyType.TIME_BLOCK, null);

        assertThat(normalized).isNull();
    }

    @Test
    @DisplayName("APP_BLOCK JSON 배열은 CSV로 정규화한다")
    void appBlockJson_normalizeToCsv() {
        String normalized =
                mapper.normalizeValue(
                        PolicyType.APP_BLOCK,
                        "{\"blockedApps\":[\"com.youtube.app\", \"com.game.app\"]}");

        assertThat(normalized).isEqualTo("com.youtube.app,com.game.app");
    }

    @Test
    @DisplayName("TIME_BLOCK JSON 필수 필드가 없으면 예외를 던진다")
    void invalidTimeBlockJson_throwException() {
        assertThatThrownBy(
                        () -> mapper.normalizeValue(PolicyType.TIME_BLOCK, "{\"start\":\"23:00\"}"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
