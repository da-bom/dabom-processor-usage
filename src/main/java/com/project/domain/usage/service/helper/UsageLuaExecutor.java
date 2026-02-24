package com.project.domain.usage.service.helper;

import java.util.List;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import com.project.domain.usage.service.dto.UsageUpdateResult;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class UsageLuaExecutor {

    private final StringRedisTemplate redisTemplate;
    private final RedisScript<List<Object>> usageUpdateScript;

    // Usage Lua 스크립트를 실행하고 결과를 도메인 DTO로 변환한다.
    public UsageUpdateResult execute(UsageLuaCommand command, String eventId) {
        List<Object> result =
                redisTemplate.execute(
                        usageUpdateScript,
                        List.of(
                                command.infoKey(),
                                command.remainingKey(),
                                command.monthlyKey(),
                                command.constraintsKey(),
                                command.alertsKey()),
                        String.valueOf(command.usageBytes()),
                        command.currentHhmm());

        if (result == null || result.isEmpty()) {
            log.error("Usage update script returned null. eventId={}", eventId);
            throw new IllegalStateException("Usage update script returned null");
        }

        return parseScriptResult(result, eventId);
    }

    // lua script 결과 파싱
    private UsageUpdateResult parseScriptResult(List<Object> result, String eventId) {
        if (result.size() < 6) {
            log.error(
                    "Usage update script returned invalid result. eventId={}, result={}",
                    eventId,
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

        return new UsageUpdateResult(
                totalUsed, remaining, status, monthlyUsed, userRatio, monthlyLimit);
    }

    // Lua 실행에 필요한 키/인자를 한 번에 전달하기 위한 내부 커맨드 객체
    public record UsageLuaCommand(
            String infoKey,
            String remainingKey,
            String monthlyKey,
            String constraintsKey,
            String alertsKey,
            long usageBytes,
            String currentHhmm) {}
}
