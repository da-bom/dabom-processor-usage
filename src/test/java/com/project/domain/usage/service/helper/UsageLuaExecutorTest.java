package com.project.domain.usage.service.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import com.project.domain.usage.service.dto.UsageUpdateResult;
import com.project.global.util.LogSanitizer;

@ExtendWith(MockitoExtension.class)
class UsageLuaExecutorTest {

    @InjectMocks private UsageLuaExecutor usageLuaExecutor;

    @Mock private StringRedisTemplate redisTemplate;
    @Mock private RedisScript<List<Object>> usageUpdateScript;
    @Mock private LogSanitizer logSanitizer;

    @BeforeEach
    void setUp() {
        lenient()
                .when(logSanitizer.sanitize(nullable(String.class)))
                .thenAnswer(
                        invocation -> {
                            String raw = invocation.getArgument(0);
                            return raw == null ? "null" : raw;
                        });
    }

    @Test
    @DisplayName("Lua 결과를 UsageUpdateResult로 파싱한다")
    void execute_ParseSuccess() {
        UsageLuaExecutor.UsageLuaCommand command =
                new UsageLuaExecutor.UsageLuaCommand(
                        "family:100:info",
                        "family:100:remaining",
                        "monthlyKey",
                        "constraintsKey",
                        "alertsKey",
                        1024L,
                        "2230");

        given(
                        redisTemplate.execute(
                                eq(usageUpdateScript),
                                anyList(),
                                any(Object.class),
                                any(Object.class)))
                .willReturn(List.of(5000L, 5000L, "NORMAL", 1000L, 0.1, 10000L));

        UsageUpdateResult result = usageLuaExecutor.execute(command, "evt_1");

        assertEquals(5000L, result.totalUsed());
        assertEquals(5000L, result.remaining());
        assertEquals("NORMAL", result.status());
        assertEquals(1000L, result.monthlyUsed());
        assertEquals(0.1, result.userRatio());
        assertEquals(10000L, result.monthlyLimit());
    }

    @Test
    @DisplayName("Lua 결과가 null이면 예외를 던진다")
    void execute_NullResult() {
        UsageLuaExecutor.UsageLuaCommand command =
                new UsageLuaExecutor.UsageLuaCommand("a", "b", "c", "d", "e", 1L, "0000");
        given(
                        redisTemplate.execute(
                                eq(usageUpdateScript),
                                anyList(),
                                any(Object.class),
                                any(Object.class)))
                .willReturn(null);

        assertThrows(IllegalStateException.class, () -> usageLuaExecutor.execute(command, "evt_2"));
    }

    @Test
    @DisplayName("Lua 결과 길이가 부족하면 예외를 던진다")
    void execute_InvalidResultSize() {
        UsageLuaExecutor.UsageLuaCommand command =
                new UsageLuaExecutor.UsageLuaCommand("a", "b", "c", "d", "e", 1L, "0000");
        given(
                        redisTemplate.execute(
                                eq(usageUpdateScript),
                                anyList(),
                                any(Object.class),
                                any(Object.class)))
                .willReturn(List.of(1L, 2L, "NORMAL"));

        assertThrows(IllegalStateException.class, () -> usageLuaExecutor.execute(command, "evt_3"));
    }
}
