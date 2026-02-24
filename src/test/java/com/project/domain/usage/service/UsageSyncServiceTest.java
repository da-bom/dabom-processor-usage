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
import com.project.domain.policy.service.helper.PolicyConstraintWarmupHelper;
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
    @Mock private PolicyConstraintWarmupHelper policyConstraintWarmupHelper;

    @Mock private UsagePersistKafkaProducer persistProducer;

    @Mock private UsageRealtimeKafkaProducer realtimeProducer;

    @Mock private NotificationKafkaProducer notificationProducer;

    @Mock private RedisScript<List<Object>> usageUpdateScript;

    @Test
    @DisplayName("정상 상태에서는 Persist와 Realtime 이벤트만 발행된다")
    void syncUsage_Normal() {
        String eventId = "evt_1";
        String eventTime = LocalDateTime.now().toString();
        UsagePayload payload = new UsagePayload(eventId, 100L, 1L, "appId", 1024L, Map.of());

        stubCommon(100L, 1L);

        List<Object> scriptResult = List.of(5000L, 5000L, "NORMAL", 1000L, 0.1, 10000L);
        given(
                        redisTemplate.execute(
                                eq(usageUpdateScript),
                                anyList(),
                                any(Object.class),
                                any(Object.class)))
                .willReturn(scriptResult);

        usageSyncService.syncUsage(eventId, eventTime, payload);

        verify(persistProducer, times(1)).publish(any(UsagePersistPayload.class));
        verify(realtimeProducer, times(1)).publish(any(UsageRealtimePayload.class));
        verify(notificationProducer, never()).publish(any(ThresholdAlertPayload.class));
        verify(notificationProducer, never()).publish(any(CustomerBlockedPayload.class));
    }

    @Test
    @DisplayName("WARNING 상태에서는 ThresholdAlertPayload가 발행된다")
    void syncUsage_Warning() {
        String eventId = "evt_2";
        String eventTime = LocalDateTime.now().toString();
        UsagePayload payload = new UsagePayload(eventId, 100L, 1L, "appId", 1024L, Map.of());

        stubCommon(100L, 1L);

        List<Object> scriptResult = List.of(9000L, 1000L, "WARNING_10", 2000L, 0.2, 10000L);
        given(
                        redisTemplate.execute(
                                eq(usageUpdateScript),
                                anyList(),
                                any(Object.class),
                                any(Object.class)))
                .willReturn(scriptResult);

        usageSyncService.syncUsage(eventId, eventTime, payload);

        verify(notificationProducer, times(1)).publish(any(ThresholdAlertPayload.class));
    }

    @Test
    @DisplayName("BLOCKED 상태에서는 CustomerBlockedPayload가 발행된다")
    void syncUsage_Blocked() {
        String eventId = "evt_3";
        String eventTime = LocalDateTime.now().toString();
        UsagePayload payload = new UsagePayload(eventId, 100L, 1L, "appId", 1024L, Map.of());

        stubCommon(100L, 1L);

        List<Object> scriptResult =
                List.of(8000L, 2000L, "BLOCKED_LIMIT_MONTHLY", 10001L, 1.0, 10000L);
        given(
                        redisTemplate.execute(
                                eq(usageUpdateScript),
                                anyList(),
                                any(Object.class),
                                any(Object.class)))
                .willReturn(scriptResult);

        usageSyncService.syncUsage(eventId, eventTime, payload);

        verify(notificationProducer, times(1)).publish(any(CustomerBlockedPayload.class));
    }

    @Test
    @DisplayName("시간 차단(BLOCKED_TIME) 상태에서는 CustomerBlockedPayload가 발행된다")
    void syncUsage_BlockedTime() {
        String eventId = "evt_4";
        String eventTime = "2026-02-20T23:30:00";
        UsagePayload payload = new UsagePayload(eventId, 100L, 1L, "appId", 1024L, Map.of());

        stubCommon(100L, 1L);

        List<Object> scriptResult = List.of(8000L, 2000L, "BLOCKED_TIME", 1000L, 0.1, 10000L);
        given(
                        redisTemplate.execute(
                                eq(usageUpdateScript),
                                anyList(),
                                any(Object.class),
                                any(Object.class)))
                .willReturn(scriptResult);

        usageSyncService.syncUsage(eventId, eventTime, payload);

        verify(notificationProducer, times(1)).publish(any(CustomerBlockedPayload.class));
    }

    private void stubCommon(long familyId, long customerId) {
        given(redisKeyGenerator.generateFamilyInfoKey(familyId))
                .willReturn("family:" + familyId + ":info");
        given(redisKeyGenerator.generateFamilyRemainingKey(familyId))
                .willReturn("family:" + familyId + ":remaining");
        given(redisKeyGenerator.generateFamilyCustomerMonthlyUsageKey(familyId, customerId))
                .willReturn("monthlyKey");
        given(redisKeyGenerator.generateFamilyCustomerConstraintsKey(familyId, customerId))
                .willReturn("constraintsKey");
        given(redisKeyGenerator.generateFamilyAlertsKey(familyId)).willReturn("alertsKey");
        given(
                        usageRedisWarmupService.ensureFamilyInfoCached(
                                familyId, "family:" + familyId + ":info"))
                .willReturn(true);
        given(
                        usageRedisWarmupService.ensureRemainingBytesCached(
                                familyId, "family:" + familyId + ":remaining"))
                .willReturn(true);
        given(usageRedisWarmupService.ensureCustomerUsageCached(familyId, customerId, "monthlyKey"))
                .willReturn(true);
    }
}
