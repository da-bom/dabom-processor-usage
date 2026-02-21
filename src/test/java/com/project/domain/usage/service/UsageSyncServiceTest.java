package com.project.domain.usage.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import com.project.domain.notification.infra.messaging.NotificationKafkaProducer;
import com.project.domain.policy.service.PolicyConstraintWarmupService;
import com.project.domain.usage.infra.messaging.UsagePersistKafkaProducer;
import com.project.domain.usage.infra.messaging.UsageRealtimeKafkaProducer;
import com.project.global.event.dto.notification.CustomerBlockedPayload;
import com.project.global.event.dto.notification.ThresholdAlertPayload;
import com.project.global.event.dto.usage.UsagePayload;
import com.project.global.event.dto.usage.UsagePersistPayload;
import com.project.global.event.dto.usage.UsageRealtimePayload;
import com.project.global.util.RedisKeyGenerator;

@ExtendWith(MockitoExtension.class)
class UsageSyncServiceTest {

    @InjectMocks private UsageSyncService usageSyncService;

    @Mock private StringRedisTemplate redisTemplate;

    @Mock private RedisKeyGenerator redisKeyGenerator;
    @Mock private UsageRedisWarmupService usageRedisWarmupService;
    @Mock private PolicyConstraintWarmupService policyConstraintWarmupService;

    @Mock private UsagePersistKafkaProducer persistProducer;

    @Mock private UsageRealtimeKafkaProducer realtimeProducer;

    @Mock private NotificationKafkaProducer notificationProducer;

    @Mock private RedisScript<List<Object>> usageUpdateScript;

    @Test
    @DisplayName("정상 상태일 때는 Persist와 Realtime 이벤트만 발행되어야 한다")
    void syncUsage_Normal() {
        // given
        String eventId = "evt_1";
        String eventTime = LocalDateTime.now().toString();
        UsagePayload payload = new UsagePayload(eventId, 100L, 1L, "appId", 1024L, Map.of());

        // Key Mocking
        given(redisKeyGenerator.generateFamilyInfoKey(100L)).willReturn("family:100:info");
        given(redisKeyGenerator.generateFamilyRemainingKey(100L))
                .willReturn("family:100:remaining");
        given(redisKeyGenerator.generateFamilyCustomerMonthlyUsageKey(100L, 1L))
                .willReturn("monthlyKey");
        given(redisKeyGenerator.generateFamilyCustomerConstraintsKey(100L, 1L))
                .willReturn("constraintsKey");
        given(redisKeyGenerator.generateFamilyAlertsKey(100L)).willReturn("alertsKey");
        given(usageRedisWarmupService.ensureFamilyInfoCached(100L, "family:100:info"))
                .willReturn(true);
        given(usageRedisWarmupService.ensureRemainingBytesCached(100L, "family:100:remaining"))
                .willReturn(true);
        given(usageRedisWarmupService.ensureCustomerUsageCached(100L, 1L, "monthlyKey"))
                .willReturn(true);

        // Mock Lua Result: [totalUsed, remaining, status, monthlyUsed, userRatio, monthlyLimit]
        List<Object> scriptResult = List.of(5000L, 5000L, "NORMAL", 1000L, 0.1, 10000L);

        // RedisTemplate execute mocking
        // 주의: varargs 매칭 등 까다로운 부분은 any() 사용 권장
        given(redisTemplate.execute(eq(usageUpdateScript), anyList(), any(Object.class)))
                .willReturn(scriptResult);

        // when
        usageSyncService.syncUsage(eventId, eventTime, payload);

        // then
        // 1. Persist 발행 확인
        verify(persistProducer, times(1)).publish(any(UsagePersistPayload.class));

        // 2. Realtime 발행 확인
        verify(realtimeProducer, times(1)).publish(any(UsageRealtimePayload.class));

        // 3. Notification 발행 안 함 확인
        verify(notificationProducer, never()).publish(any(ThresholdAlertPayload.class));
        verify(notificationProducer, never()).publish(any(CustomerBlockedPayload.class));
    }

    @Test
    @DisplayName("WARNING 상태일 때는 ThresholdAlertPayload가 발행되어야 한다")
    void syncUsage_Warning() {
        // given
        String eventId = "evt_2";
        String eventTime = LocalDateTime.now().toString();
        UsagePayload payload = new UsagePayload(eventId, 100L, 1L, "appId", 1024L, Map.of());

        given(redisKeyGenerator.generateFamilyInfoKey(100L)).willReturn("family:100:info");
        given(redisKeyGenerator.generateFamilyRemainingKey(100L))
                .willReturn("family:100:remaining");
        given(redisKeyGenerator.generateFamilyCustomerMonthlyUsageKey(100L, 1L))
                .willReturn("monthlyKey");
        given(redisKeyGenerator.generateFamilyCustomerConstraintsKey(100L, 1L))
                .willReturn("constraintsKey");
        given(redisKeyGenerator.generateFamilyAlertsKey(100L)).willReturn("alertsKey");
        given(usageRedisWarmupService.ensureFamilyInfoCached(100L, "family:100:info"))
                .willReturn(true);
        given(usageRedisWarmupService.ensureRemainingBytesCached(100L, "family:100:remaining"))
                .willReturn(true);
        given(usageRedisWarmupService.ensureCustomerUsageCached(100L, 1L, "monthlyKey"))
                .willReturn(true);

        // Status: WARNING_10 (10% 남음)
        // [totalUsed, remaining, status, monthlyUsed, userRatio, monthlyLimit]
        List<Object> scriptResult = List.of(9000L, 1000L, "WARNING_10", 2000L, 0.2, 10000L);

        given(redisTemplate.execute(eq(usageUpdateScript), anyList(), any(Object.class)))
                .willReturn(scriptResult);

        // when
        usageSyncService.syncUsage(eventId, eventTime, payload);

        // then
        verify(notificationProducer, times(1)).publish(any(ThresholdAlertPayload.class));
    }

    @Test
    @DisplayName("BLOCKED 상태일 때는 CustomerBlockedPayload가 발행되어야 한다")
    void syncUsage_Blocked() {
        // given
        String eventId = "evt_3";
        String eventTime = LocalDateTime.now().toString();
        UsagePayload payload = new UsagePayload(eventId, 100L, 1L, "appId", 1024L, Map.of());

        given(redisKeyGenerator.generateFamilyInfoKey(100L)).willReturn("family:100:info");
        given(redisKeyGenerator.generateFamilyRemainingKey(100L))
                .willReturn("family:100:remaining");
        given(redisKeyGenerator.generateFamilyCustomerMonthlyUsageKey(100L, 1L))
                .willReturn("monthlyKey");
        given(redisKeyGenerator.generateFamilyCustomerConstraintsKey(100L, 1L))
                .willReturn("constraintsKey");
        given(redisKeyGenerator.generateFamilyAlertsKey(100L)).willReturn("alertsKey");
        given(usageRedisWarmupService.ensureFamilyInfoCached(100L, "family:100:info"))
                .willReturn(true);
        given(usageRedisWarmupService.ensureRemainingBytesCached(100L, "family:100:remaining"))
                .willReturn(true);
        given(usageRedisWarmupService.ensureCustomerUsageCached(100L, 1L, "monthlyKey"))
                .willReturn(true);

        // Status: BLOCKED_LIMIT_MONTHLY
        // [totalUsed, remaining, status, monthlyUsed, userRatio, monthlyLimit]
        List<Object> scriptResult =
                List.of(8000L, 2000L, "BLOCKED_LIMIT_MONTHLY", 10001L, 1.0, 10000L);

        given(redisTemplate.execute(eq(usageUpdateScript), anyList(), any(Object.class)))
                .willReturn(scriptResult);

        // when
        usageSyncService.syncUsage(eventId, eventTime, payload);

        // then
        verify(notificationProducer, times(1)).publish(any(CustomerBlockedPayload.class));
    }
}
