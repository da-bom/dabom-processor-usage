package com.project.domain.usage.service.helper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.dabom.messaging.kafka.error.NonRetryableKafkaMessageProcessingException;
import com.project.domain.usage.enums.UsagePersistProcessResult;

class UsageProcessingDecisionMapperTest {

    private final UsageProcessingDecisionMapper mapper = new UsageProcessingDecisionMapper();

    @Test
    @DisplayName("WARNING 상태는 ALLOWED 정산과 notification 발행 대상으로 해석한다")
    void warningStatus() {
        UsageProcessingDecisionMapper.UsageProcessingDecision decision =
                mapper.fromLuaStatus("WARNING_10");

        assertThat(decision.persistProcessResult())
                .isEqualTo(UsagePersistProcessResult.ALLOWED.name());
        assertThat(decision.publishNotification()).isTrue();
        assertThat(decision.notificationStatus()).isEqualTo("WARNING_10");
    }

    @Test
    @DisplayName("차단 상태는 차단 정산과 notification 발행 대상으로 해석한다")
    void blockedStatus() {
        UsageProcessingDecisionMapper.UsageProcessingDecision decision =
                mapper.fromLuaStatus("APP_BLOCK");

        assertThat(decision.persistProcessResult()).isEqualTo("APP_BLOCK");
        assertThat(decision.publishNotification()).isTrue();
        assertThat(decision.notificationStatus()).isEqualTo("APP_BLOCK");
    }

    @Test
    @DisplayName("알 수 없는 상태는 즉시 DLQ 대상 예외로 처리한다")
    void unknownStatus() {
        assertThrows(
                NonRetryableKafkaMessageProcessingException.class,
                () -> mapper.fromLuaStatus("NEW_STATUS"));
    }
}
