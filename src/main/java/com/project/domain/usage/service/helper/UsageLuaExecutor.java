package com.project.domain.usage.service.helper;

import java.util.List;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import com.dabom.messaging.kafka.error.KafkaMessageProcessingException;
import com.project.domain.usage.service.dto.UsageUpdateResult;
import com.project.global.util.LogSanitizer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class UsageLuaExecutor {

    private final StringRedisTemplate redisTemplate;
    private final RedisScript<List<Object>> usageUpdateScript;
    private final LogSanitizer logSanitizer;

    // usage Lua를 실행하고 결과를 파싱한다.
    public UsageUpdateResult execute(UsageLuaCommand command, String eventId) {
        List<Object> result =
                redisTemplate.execute(
                        usageUpdateScript,
                        List.of(
                                command.infoKey(),
                                command.remainingKey(),
                                command.monthlyKey(),
                                command.constraintsKey(),
                                command.alert50Key(),
                                command.alert30Key(),
                                command.alert10Key(),
                                command.manualAlertKey(),
                                command.appBlockAlertKey(),
                                command.timeBlockAlertKey(),
                                command.monthlyLimitAlertKey(),
                                command.familyQuotaAlertKey(),
                                command.dedupKey()),
                        String.valueOf(command.usageBytes()),
                        command.currentHhmm(),
                        command.appId(),
                        String.valueOf(command.dedupTtlSeconds()));

        if (result == null || result.isEmpty()) {
            log.error("Usage update script returned null. eventId={}", eventId);
            throw new KafkaMessageProcessingException(
                    "Usage Lua returned null. eventId=%s".formatted(eventId),
                    new IllegalStateException("Usage update script returned null"));
        }

        return parseScriptResult(result, eventId);
    }

    // Lua 결과 배열을 UsageUpdateResult로 변환한다.
    private UsageUpdateResult parseScriptResult(List<Object> result, String eventId) {
        if (result.size() < 8) {
            log.error(
                    "Usage update script returned invalid result. eventId={}, result={}",
                    logSanitizer.sanitize(eventId),
                    result);
            throw new KafkaMessageProcessingException(
                    "Usage Lua returned invalid result. eventId=%s".formatted(eventId),
                    new IllegalStateException("Invalid Lua script result"));
        }

        long totalUsed = ((Number) result.get(0)).longValue();
        long remaining = ((Number) result.get(1)).longValue();
        String status = (String) result.get(2);
        long monthlyUsed = ((Number) result.get(3)).longValue();

        Object userRatioObj = result.get(4);
        double userRatio =
                (userRatioObj instanceof Number number)
                        ? number.doubleValue()
                        : Double.parseDouble(userRatioObj.toString());

        long monthlyLimit = ((Number) result.get(5)).longValue();
        boolean shouldNotify = ((Number) result.get(6)).longValue() == 1L;
        boolean duplicate = ((Number) result.get(7)).longValue() == 1L;

        return new UsageUpdateResult(
                totalUsed,
                remaining,
                status,
                monthlyUsed,
                userRatio,
                monthlyLimit,
                shouldNotify,
                duplicate);
    }

    // Lua 실행에 필요한 인자를 묶는다.
    public record UsageLuaCommand(
            String infoKey,
            String remainingKey,
            String monthlyKey,
            String constraintsKey,
            String alert50Key,
            String alert30Key,
            String alert10Key,
            String manualAlertKey,
            String appBlockAlertKey,
            String timeBlockAlertKey,
            String monthlyLimitAlertKey,
            String familyQuotaAlertKey,
            String dedupKey,
            long usageBytes,
            String currentHhmm,
            String appId,
            long dedupTtlSeconds) {}
}
