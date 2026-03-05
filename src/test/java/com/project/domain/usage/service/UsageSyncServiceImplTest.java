package com.project.domain.usage.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.LocalDate;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.project.domain.policy.service.helper.PolicyConstraintWarmupHelper;
import com.project.domain.usage.service.dto.UsageUpdateResult;
import com.project.domain.usage.service.helper.UsageEventPublisher;
import com.project.domain.usage.service.helper.UsageLuaExecutor;
import com.project.domain.usage.service.helper.UsageRedisWarmupHelper;
import com.project.global.event.dto.usage.UsagePayload;
import com.project.global.util.LogSanitizer;
import com.project.global.util.RedisKeyGenerator;

@ExtendWith(MockitoExtension.class)
class UsageSyncServiceImplTest {

    @InjectMocks private UsageSyncServiceImpl usageSyncServiceImpl;

    @Mock private RedisKeyGenerator redisKeyGenerator;
    @Mock private UsageRedisWarmupHelper usageRedisWarmupHelper;
    @Mock private PolicyConstraintWarmupHelper policyConstraintWarmupHelper;
    @Mock private UsageLuaExecutor usageLuaExecutor;
    @Mock private UsageEventPublisher usageEventPublisher;
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
    @DisplayName("정상 흐름이면 Lua 실행 후 이벤트 발행기로 위임한다")
    void syncUsage_SuccessFlow() {
        String eventId = "evt_1";
        String eventTime = "2026-03-04T12:34:56";
        LocalDate eventMonth = LocalDate.of(2026, 3, 1);
        UsagePayload payload = new UsagePayload(100L, 1L, "appId", 1024L, Map.of());

        stubCommon(100L, 1L, eventMonth);

        UsageUpdateResult luaResult =
                new UsageUpdateResult(5000L, 5000L, "NORMAL", 1000L, 0.1, 10000L);
        given(usageLuaExecutor.execute(any(UsageLuaExecutor.UsageLuaCommand.class), eq(eventId)))
                .willReturn(luaResult);

        usageSyncServiceImpl.syncUsage(eventId, eventTime, payload);

        ArgumentCaptor<UsageLuaExecutor.UsageLuaCommand> commandCaptor =
                ArgumentCaptor.forClass(UsageLuaExecutor.UsageLuaCommand.class);
        verify(usageLuaExecutor, times(1)).execute(commandCaptor.capture(), eq(eventId));

        UsageLuaExecutor.UsageLuaCommand command = commandCaptor.getValue();
        assertEquals("family:100:info", command.infoKey());
        assertEquals("family:100:remaining", command.remainingKey());
        assertEquals("monthlyKey", command.monthlyKey());
        assertEquals("constraintsKey", command.constraintsKey());
        assertEquals("alertsKey", command.alertsKey());
        assertEquals(1024L, command.usageBytes());
        assertEquals("1234", command.currentHhmm());

        verify(usageEventPublisher, times(1))
                .publish(any(UsageEventPublisher.UsageEventContext.class));
    }

    @Test
    @DisplayName("Warmup 실패 시 Lua 실행과 이벤트 발행을 하지 않는다")
    void syncUsage_WarmupFailed() {
        String eventId = "evt_2";
        String eventTime = "2026-03-04T10:10:10";
        LocalDate eventMonth = LocalDate.of(2026, 3, 1);
        UsagePayload payload = new UsagePayload(100L, 1L, "appId", 1024L, Map.of());

        given(redisKeyGenerator.generateFamilyInfoKey(100L)).willReturn("family:100:info");
        given(redisKeyGenerator.generateFamilyRemainingKey(100L))
                .willReturn("family:100:remaining");
        given(redisKeyGenerator.generateFamilyCustomerMonthlyUsageKey(100L, 1L, eventMonth))
                .willReturn("monthlyKey");
        given(redisKeyGenerator.generateFamilyCustomerConstraintsKey(100L, 1L))
                .willReturn("constraintsKey");
        given(redisKeyGenerator.generateFamilyAlertsKey(100L)).willReturn("alertsKey");

        given(usageRedisWarmupHelper.ensureFamilyInfoCached(100L, "family:100:info"))
                .willReturn(false);
        given(usageRedisWarmupHelper.ensureRemainingBytesCached(100L, "family:100:remaining"))
                .willReturn(true);
        given(usageRedisWarmupHelper.ensureCustomerUsageCached(100L, 1L, "monthlyKey", eventMonth))
                .willReturn(true);

        usageSyncServiceImpl.syncUsage(eventId, eventTime, payload);

        verify(usageLuaExecutor, never()).execute(any(), any());
        verify(usageEventPublisher, never()).publish(any());
    }

    private void stubCommon(long familyId, long customerId, LocalDate eventMonth) {
        given(redisKeyGenerator.generateFamilyInfoKey(familyId))
                .willReturn("family:" + familyId + ":info");
        given(redisKeyGenerator.generateFamilyRemainingKey(familyId))
                .willReturn("family:" + familyId + ":remaining");
        given(
                        redisKeyGenerator.generateFamilyCustomerMonthlyUsageKey(
                                familyId, customerId, eventMonth))
                .willReturn("monthlyKey");
        given(redisKeyGenerator.generateFamilyCustomerConstraintsKey(familyId, customerId))
                .willReturn("constraintsKey");
        given(redisKeyGenerator.generateFamilyAlertsKey(familyId)).willReturn("alertsKey");
        given(
                        usageRedisWarmupHelper.ensureFamilyInfoCached(
                                familyId, "family:" + familyId + ":info"))
                .willReturn(true);
        given(
                        usageRedisWarmupHelper.ensureRemainingBytesCached(
                                familyId, "family:" + familyId + ":remaining"))
                .willReturn(true);
        given(
                        usageRedisWarmupHelper.ensureCustomerUsageCached(
                                familyId, customerId, "monthlyKey", eventMonth))
                .willReturn(true);
    }
}
