package com.project.domain.usage.service.helper;

import org.springframework.stereotype.Component;

import com.dabom.messaging.kafka.error.NonRetryableKafkaMessageProcessingException;
import com.project.domain.usage.enums.UsagePersistProcessResult;

@Component
public class UsageProcessingDecisionMapper {

    // Lua 상태 문자열을 DB 정산/알림 판단용 결정으로 변환한다.
    public UsageProcessingDecision fromLuaStatus(String rawStatus) {
        if (rawStatus == null || rawStatus.isBlank()) {
            throw new NonRetryableKafkaMessageProcessingException("Lua status is null or blank");
        }

        // 허용/경고/차단 상태를 명시적으로만 해석한다.
        return switch (rawStatus) {
            case "NORMAL" ->
                    new UsageProcessingDecision(
                            UsagePersistProcessResult.ALLOWED.name(), false, rawStatus);
            case "WARNING_50", "WARNING_30", "WARNING_10" ->
                    new UsageProcessingDecision(
                            UsagePersistProcessResult.ALLOWED.name(), true, rawStatus);
            case "MANUAL",
                            "APP_BLOCK",
                            "TIME_BLOCK",
                            "MONTHLY_LIMIT_EXCEEDED",
                            "FAMILY_QUOTA_EXCEEDED" ->
                    new UsageProcessingDecision(rawStatus, true, rawStatus);
            default ->
                    throw new NonRetryableKafkaMessageProcessingException(
                            "Unsupported Lua status: " + rawStatus);
        };
    }

    // usage 처리 결과를 각 후속 단계가 공통으로 참조하는 결정 객체다.
    public record UsageProcessingDecision(
            String persistProcessResult, boolean publishNotification, String notificationStatus) {}
}
