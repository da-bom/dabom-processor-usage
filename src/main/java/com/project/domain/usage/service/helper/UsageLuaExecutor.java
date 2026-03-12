package com.project.domain.usage.service.helper;

import java.util.List;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

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

    // usage Lua 실행 + 결과 파싱
    public UsageUpdateResult execute(UsageLuaCommand command, String eventId) {
        List<Object> result =
                redisTemplate.execute(
                        usageUpdateScript,
                        List.of(
                                command.infoKey(),
                                command.remainingKey(),
                                command.monthlyKey(),
                                command.constraintsKey(),
                                command.alertsKey(),
                                command.dedupKey()),
                        String.valueOf(command.usageBytes()),
                        command.currentHhmm(),
                        command.appId(),
                        String.valueOf(command.dedupTtlSeconds()));

        if (result == null || result.isEmpty()) {
            log.error("Usage update script returned null. eventId={}", eventId);
            throw new IllegalStateException("Usage update script returned null");
        }

        return parseScriptResult(result, eventId);
    }

    // Lua 결과 파싱
    private UsageUpdateResult parseScriptResult(List<Object> result, String eventId) {
        if (result.size() < 7) {
            log.error(
                    "Usage update script returned invalid result. eventId={}, result={}",
                    logSanitizer.sanitize(eventId),
                    result);
            throw new IllegalStateException("Invalid Lua script result");
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
        // 마지막 값은 duplicate 여부
        boolean duplicate = ((Number) result.get(6)).longValue() == 1L;

        return new UsageUpdateResult(
                totalUsed, remaining, status, monthlyUsed, userRatio, monthlyLimit, duplicate);
    }

    // Lua 실행에 필요한 인자 묶음
    public record UsageLuaCommand(
            String infoKey,
            String remainingKey,
            String monthlyKey,
            String constraintsKey,
            String alertsKey,
            // usage-event 중복 검사 키
            String dedupKey,
            long usageBytes,
            String currentHhmm,
            String appId,
            long dedupTtlSeconds) {}
}
