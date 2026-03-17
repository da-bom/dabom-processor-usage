package com.project.domain.usage.service.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import com.dabom.messaging.kafka.error.KafkaMessageProcessingException;
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
                        invocation ->
                                invocation.getArgument(0) == null
                                        ? "null"
                                        : invocation.getArgument(0));
    }

    @Test
    @DisplayName("Lua 결과를 UsageUpdateResult로 파싱한다")
    void execute_ParseSuccess() {
        UsageLuaExecutor.UsageLuaCommand command =
                new UsageLuaExecutor.UsageLuaCommand(
                        "family:100:info:202603",
                        "family:100:remaining:202603",
                        "monthlyKey",
                        "constraintsKey",
                        "family:100:customer:1:alert:THRESHOLD:50:202603",
                        "family:100:customer:1:alert:THRESHOLD:30:202603",
                        "family:100:customer:1:alert:THRESHOLD:10:202603",
                        "family:100:customer:1:alert:MANUAL:202603",
                        "family:100:customer:1:alert:APP_BLOCK:com.youtube.app:202603",
                        "family:100:customer:1:alert:TIME_BLOCK:202603",
                        "family:100:customer:1:alert:MONTHLY_LIMIT_EXCEEDED:202603",
                        "family:100:customer:1:alert:FAMILY_QUOTA_EXCEEDED:202603",
                        "event:dedup:usage:evt_1",
                        1024L,
                        "2230",
                        "com.youtube.app",
                        60L);

        given(
                        redisTemplate.execute(
                                eq(usageUpdateScript),
                                anyList(),
                                any(Object.class),
                                any(Object.class),
                                any(Object.class),
                                any(Object.class)))
                .willReturn(List.of(5000L, 5000L, "WARNING_10", 1000L, 0.1, 10000L, 1L, 0L));

        UsageUpdateResult result = usageLuaExecutor.execute(command, "evt_1");

        ArgumentCaptor<List> keysCaptor = ArgumentCaptor.forClass(List.class);
        verify(redisTemplate)
                .execute(
                        eq(usageUpdateScript),
                        keysCaptor.capture(),
                        eq("1024"),
                        eq("2230"),
                        eq("com.youtube.app"),
                        eq("60"));

        assertEquals(13, keysCaptor.getValue().size());
        assertEquals(
                "family:100:customer:1:alert:FAMILY_QUOTA_EXCEEDED:202603",
                keysCaptor.getValue().get(11));
        assertEquals("event:dedup:usage:evt_1", keysCaptor.getValue().get(12));
        assertEquals(5000L, result.totalUsed());
        assertEquals(5000L, result.remaining());
        assertEquals("WARNING_10", result.status());
        assertEquals(1000L, result.monthlyUsed());
        assertEquals(0.1, result.userRatio());
        assertEquals(10000L, result.monthlyLimit());
        assertTrue(result.shouldNotify());
        assertFalse(result.duplicate());
    }

    @Test
    @DisplayName("duplicate와 notify 플래그를 함께 파싱한다")
    void execute_ParseDuplicateFlag() {
        UsageLuaExecutor.UsageLuaCommand command =
                new UsageLuaExecutor.UsageLuaCommand(
                        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "dup", 1L,
                        "0000", "", 60L);
        given(
                        redisTemplate.execute(
                                eq(usageUpdateScript),
                                anyList(),
                                any(Object.class),
                                any(Object.class),
                                any(Object.class),
                                any(Object.class)))
                .willReturn(List.of(100L, 900L, "APP_BLOCK", 50L, 0.05, -1L, 0L, 1L));

        UsageUpdateResult result = usageLuaExecutor.execute(command, "evt_dup");

        assertTrue(result.duplicate());
        assertFalse(result.shouldNotify());
        assertEquals("APP_BLOCK", result.status());
    }

    @Test
    @DisplayName("Lua 결과가 null이면 예외를 던진다")
    void execute_NullResult() {
        UsageLuaExecutor.UsageLuaCommand command =
                new UsageLuaExecutor.UsageLuaCommand(
                        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "dup", 1L,
                        "0000", "", 60L);
        given(
                        redisTemplate.execute(
                                eq(usageUpdateScript),
                                anyList(),
                                any(Object.class),
                                any(Object.class),
                                any(Object.class),
                                any(Object.class)))
                .willReturn(null);

        assertThrows(
                KafkaMessageProcessingException.class,
                () -> usageLuaExecutor.execute(command, "evt_2"));
    }

    @Test
    @DisplayName("Lua 결과 길이가 부족하면 예외를 던진다")
    void execute_InvalidResultSize() {
        UsageLuaExecutor.UsageLuaCommand command =
                new UsageLuaExecutor.UsageLuaCommand(
                        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "dup", 1L,
                        "0000", "", 60L);
        given(
                        redisTemplate.execute(
                                eq(usageUpdateScript),
                                anyList(),
                                any(Object.class),
                                any(Object.class),
                                any(Object.class),
                                any(Object.class)))
                .willReturn(List.of(1L, 2L, "NORMAL"));

        assertThrows(
                KafkaMessageProcessingException.class,
                () -> usageLuaExecutor.execute(command, "evt_3"));
    }
}
