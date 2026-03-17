package com.project.domain.usage.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import com.dabom.messaging.kafka.contract.KafkaConsumerGroups;
import com.dabom.messaging.kafka.contract.KafkaEventTypes;
import com.dabom.messaging.kafka.contract.KafkaTopics;
import com.dabom.messaging.kafka.error.KafkaMessageProcessingException;
import com.dabom.messaging.kafka.error.NonRetryableKafkaMessageProcessingException;
import com.dabom.messaging.kafka.event.dto.notification.NotificationPayload;
import com.dabom.messaging.kafka.event.dto.notification.NotificationType;
import com.dabom.messaging.kafka.event.dto.usage.UsagePayload;
import com.dabom.messaging.kafka.metrics.KafkaMetrics;
import com.project.common.util.LogSanitizer;
import com.project.common.util.RedisKeyGenerator;
import com.project.domain.policy.helper.PolicyConstraintWarmupHelper;
import com.project.domain.usage.service.dto.UsageUpdateResult;
import com.project.domain.usage.service.helper.UsageEventOutboxService;
import com.project.domain.usage.service.helper.UsageFamilyMembershipCacheHelper;
import com.project.domain.usage.service.helper.UsageLuaExecutor;
import com.project.domain.usage.service.helper.UsageNotificationPayloadMapper;
import com.project.domain.usage.service.helper.UsageNotificationPublisher;
import com.project.domain.usage.service.helper.UsageProcessingDecisionMapper;
import com.project.domain.usage.service.helper.UsageRedisWarmupHelper;

@ExtendWith(MockitoExtension.class)
class UsageSyncServiceImplTest {

    @InjectMocks private UsageSyncServiceImpl usageSyncServiceImpl;

    @Mock private RedisKeyGenerator redisKeyGenerator;
    @Mock private UsageRedisWarmupHelper usageRedisWarmupHelper;
    @Mock private PolicyConstraintWarmupHelper policyConstraintWarmupHelper;
    @Mock private UsageLuaExecutor usageLuaExecutor;
    @Mock private UsagePersistService usagePersistService;
    @Mock private UsageEventOutboxService usageEventOutboxService;
    @Mock private UsageProcessingDecisionMapper usageProcessingDecisionMapper;
    @Mock private UsageNotificationPayloadMapper usageNotificationPayloadMapper;
    @Mock private UsageNotificationPublisher usageNotificationPublisher;
    @Mock private UsageFamilyMembershipCacheHelper usageFamilyMembershipCacheHelper;
    @Mock private LogSanitizer logSanitizer;
    @Mock private KafkaMetrics kafkaMetrics;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(usageSyncServiceImpl, "dedupTtlSeconds", 60L);
        lenient()
                .when(logSanitizer.sanitize(nullable(String.class)))
                .thenAnswer(
                        invocation ->
                                invocation.getArgument(0) == null
                                        ? "null"
                                        : invocation.getArgument(0));
    }

    @Test
    @DisplayName("정상 이벤트면 DB 정산 후 notification을 비동기로 발행한다")
    void syncUsage_SuccessFlow() {
        String eventId = "evt_1";
        String eventTime = "2026-03-04T12:34:56";
        LocalDate eventMonth = LocalDate.of(2026, 3, 1);
        UsagePayload payload = new UsagePayload(100L, 1L, "appId", 1024L, Map.of());
        UsageProcessingDecisionMapper.UsageProcessingDecision decision =
                new UsageProcessingDecisionMapper.UsageProcessingDecision(
                        "ALLOWED", true, "WARNING_10");
        NotificationPayload notificationPayload =
                new NotificationPayload(
                        100L, 1L, NotificationType.THRESHOLD_ALERT, "title", "message", Map.of());

        stubCommon(100L, 1L, eventMonth, eventId, "appid");
        given(usageLuaExecutor.execute(any(UsageLuaExecutor.UsageLuaCommand.class), eq(eventId)))
                .willReturn(
                        new UsageUpdateResult(
                                5000L, 5000L, "WARNING_10", 1000L, 0.1, 10000L, true, false));
        given(usageProcessingDecisionMapper.fromLuaStatus("WARNING_10")).willReturn(decision);
        given(
                        usageNotificationPayloadMapper.toNotificationPayload(
                                any(), any(), any(), eq("WARNING_10")))
                .willReturn(notificationPayload);
        given(usageEventOutboxService.stageAfterRedisApplied(eventId, notificationPayload, true))
                .willReturn(
                        Optional.of(
                                new UsageEventOutboxService.PendingNotificationDispatch(
                                        11L, notificationPayload)));
        given(usageNotificationPublisher.publishAsync(notificationPayload))
                .willReturn(CompletableFuture.completedFuture(null));

        usageSyncServiceImpl.syncUsage(eventId, eventTime, payload);

        ArgumentCaptor<UsageLuaExecutor.UsageLuaCommand> commandCaptor =
                ArgumentCaptor.forClass(UsageLuaExecutor.UsageLuaCommand.class);
        verify(usageLuaExecutor).execute(commandCaptor.capture(), eq(eventId));

        UsageLuaExecutor.UsageLuaCommand command = commandCaptor.getValue();
        assertEquals("family:100:customer:1:alert:THRESHOLD:50:202603", command.alert50Key());
        assertEquals("family:100:customer:1:alert:THRESHOLD:30:202603", command.alert30Key());
        assertEquals("family:100:customer:1:alert:THRESHOLD:10:202603", command.alert10Key());
        assertEquals("family:100:customer:1:alert:MANUAL:202603", command.manualAlertKey());
        assertEquals(
                "family:100:customer:1:alert:APP_BLOCK:appid:202603", command.appBlockAlertKey());
        assertEquals("family:100:customer:1:alert:TIME_BLOCK:202603", command.timeBlockAlertKey());
        assertEquals(
                "family:100:customer:1:alert:MONTHLY_LIMIT_EXCEEDED:202603",
                command.monthlyLimitAlertKey());
        assertEquals(
                "family:100:customer:1:alert:FAMILY_QUOTA_EXCEEDED:202603",
                command.familyQuotaAlertKey());

        verify(usagePersistService).persistFromUsageEvent(eventId, eventTime, payload, "ALLOWED");
        verify(usageNotificationPublisher).publishAsync(notificationPayload);
        verify(usageEventOutboxService).markSent(11L);
    }

    @Test
    @DisplayName("가족 구성원 관계가 다르면 초입에서 즉시 중단한다")
    void syncUsage_InvalidFamilyMembershipThrows() {
        UsagePayload payload = new UsagePayload(100L, 1L, "appId", 1024L, Map.of());
        given(usageFamilyMembershipCacheHelper.isValidFamilyCustomer(100L, 1L)).willReturn(false);

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        usageSyncServiceImpl.syncUsage(
                                "evt_invalid", "2026-03-04T12:34:56", payload));

        verify(usageLuaExecutor, never()).execute(any(), any());
        verify(usagePersistService, never()).persistFromUsageEvent(any(), any(), any(), any());
    }

    @Test
    @DisplayName("membership 검증 중 인프라 실패가 나면 retryable 예외로 전파한다")
    void syncUsage_MembershipLookupFailureThrowsRetryableException() {
        UsagePayload payload = new UsagePayload(100L, 1L, "appId", 1024L, Map.of());
        given(usageFamilyMembershipCacheHelper.isValidFamilyCustomer(100L, 1L))
                .willThrow(
                        new KafkaMessageProcessingException(
                                "Family membership lookup failed. familyId=100 customerId=1"
                                        + " key=family:100:members",
                                new RuntimeException("db unavailable")));

        assertThrows(
                KafkaMessageProcessingException.class,
                () ->
                        usageSyncServiceImpl.syncUsage(
                                "evt_membership_retry", "2026-03-04T12:34:56", payload));

        verify(usageLuaExecutor, never()).execute(any(), any());
    }

    @Test
    @DisplayName("중복 이벤트여도 DB 정산은 멱등하게 다시 진입한다")
    void syncUsage_DuplicateStillReentersPersist() {
        String eventId = "evt_dup";
        String eventTime = "2026-03-04T12:34:56";
        LocalDate eventMonth = LocalDate.of(2026, 3, 1);
        UsagePayload payload = new UsagePayload(100L, 1L, "appId", 1024L, Map.of());
        UsageProcessingDecisionMapper.UsageProcessingDecision decision =
                new UsageProcessingDecisionMapper.UsageProcessingDecision(
                        "ALLOWED", true, "WARNING_10");
        NotificationPayload notificationPayload =
                new NotificationPayload(
                        100L, 1L, NotificationType.THRESHOLD_ALERT, "title", "message", Map.of());

        stubCommon(100L, 1L, eventMonth, eventId, "appid");
        given(usageLuaExecutor.execute(any(UsageLuaExecutor.UsageLuaCommand.class), eq(eventId)))
                .willReturn(
                        new UsageUpdateResult(
                                5000L, 5000L, "WARNING_10", 1000L, 0.1, 10000L, true, true));
        given(usageProcessingDecisionMapper.fromLuaStatus("WARNING_10")).willReturn(decision);
        given(
                        usageNotificationPayloadMapper.toNotificationPayload(
                                any(), any(), any(), eq("WARNING_10")))
                .willReturn(notificationPayload);
        given(usageEventOutboxService.stageAfterRedisApplied(eventId, notificationPayload, true))
                .willReturn(
                        Optional.of(
                                new UsageEventOutboxService.PendingNotificationDispatch(
                                        21L, notificationPayload)));
        given(usageNotificationPublisher.publishAsync(notificationPayload))
                .willReturn(CompletableFuture.completedFuture(null));

        usageSyncServiceImpl.syncUsage(eventId, eventTime, payload);

        verify(usagePersistService).persistFromUsageEvent(eventId, eventTime, payload, "ALLOWED");
        verify(kafkaMetrics)
                .incrementDedupHit(
                        KafkaTopics.USAGE_EVENTS,
                        KafkaConsumerGroups.DABOM_PROCESSOR_USAGE_MAIN,
                        KafkaEventTypes.DATA_USAGE);
        verify(usageNotificationPublisher).publishAsync(notificationPayload);
    }

    @Test
    @DisplayName("중복 이벤트이고 이미 pending notification이 있으면 다시 즉시 발행을 시도한다")
    void syncUsage_DuplicateRepublishesExistingPendingNotification() {
        String eventId = "evt_dup_pending";
        String eventTime = "2026-03-04T12:34:56";
        LocalDate eventMonth = LocalDate.of(2026, 3, 1);
        UsagePayload payload = new UsagePayload(100L, 1L, "appId", 1024L, Map.of());
        UsageProcessingDecisionMapper.UsageProcessingDecision decision =
                new UsageProcessingDecisionMapper.UsageProcessingDecision(
                        "ALLOWED", false, "NORMAL");
        NotificationPayload notificationPayload =
                new NotificationPayload(
                        100L, 1L, NotificationType.THRESHOLD_ALERT, "title", "message", Map.of());

        stubCommon(100L, 1L, eventMonth, eventId, "appid");
        given(usageLuaExecutor.execute(any(UsageLuaExecutor.UsageLuaCommand.class), eq(eventId)))
                .willReturn(
                        new UsageUpdateResult(
                                5000L, 5000L, "NORMAL", 1000L, 0.1, 10000L, false, true));
        given(usageProcessingDecisionMapper.fromLuaStatus("NORMAL")).willReturn(decision);
        given(usageEventOutboxService.findPendingDispatchByEventId(eventId))
                .willReturn(
                        Optional.of(
                                new UsageEventOutboxService.PendingNotificationDispatch(
                                        31L, notificationPayload)));
        given(usageNotificationPublisher.publishAsync(notificationPayload))
                .willReturn(CompletableFuture.completedFuture(null));

        usageSyncServiceImpl.syncUsage(eventId, eventTime, payload);

        verify(usagePersistService).persistFromUsageEvent(eventId, eventTime, payload, "ALLOWED");
        verify(usageEventOutboxService, never()).stageAfterRedisApplied(any(), any(), anyBoolean());
        verify(usageNotificationPublisher).publishAsync(notificationPayload);
        verify(usageEventOutboxService).markSent(31L);
    }

    @Test
    @DisplayName("알림 dedup에 걸리면 DB 정산만 수행하고 outbox는 만들지 않는다")
    void syncUsage_SkipsNotificationWhenShouldNotifyFalse() {
        String eventId = "evt_skip_notify";
        String eventTime = "2026-03-04T12:34:56";
        LocalDate eventMonth = LocalDate.of(2026, 3, 1);
        UsagePayload payload = new UsagePayload(100L, 1L, "appId", 1024L, Map.of());
        UsageProcessingDecisionMapper.UsageProcessingDecision decision =
                new UsageProcessingDecisionMapper.UsageProcessingDecision(
                        "APP_BLOCK", true, "APP_BLOCK");

        stubCommon(100L, 1L, eventMonth, eventId, "appid");
        given(usageLuaExecutor.execute(any(UsageLuaExecutor.UsageLuaCommand.class), eq(eventId)))
                .willReturn(
                        new UsageUpdateResult(
                                5000L, 5000L, "APP_BLOCK", 1000L, 0.1, 10000L, false, false));
        given(usageProcessingDecisionMapper.fromLuaStatus("APP_BLOCK")).willReturn(decision);
        given(usageEventOutboxService.findPendingDispatchByEventId(eventId))
                .willReturn(Optional.empty());

        usageSyncServiceImpl.syncUsage(eventId, eventTime, payload);

        verify(usagePersistService).persistFromUsageEvent(eventId, eventTime, payload, "APP_BLOCK");
        verify(usageNotificationPayloadMapper, never())
                .toNotificationPayload(any(), any(), any(), any());
        verify(usageEventOutboxService, never()).stageAfterRedisApplied(any(), any(), anyBoolean());
        verify(usageNotificationPublisher, never()).publishAsync(any());
    }

    @Test
    @DisplayName("NORMAL 이벤트는 notification payload를 만들지 않고 끝난다")
    void syncUsage_NormalEventSkipsPayloadCreation() {
        String eventId = "evt_normal";
        String eventTime = "2026-03-04T12:34:56";
        LocalDate eventMonth = LocalDate.of(2026, 3, 1);
        UsagePayload payload = new UsagePayload(100L, 1L, "appId", 1024L, Map.of());
        UsageProcessingDecisionMapper.UsageProcessingDecision decision =
                new UsageProcessingDecisionMapper.UsageProcessingDecision(
                        "ALLOWED", false, "NORMAL");

        stubCommon(100L, 1L, eventMonth, eventId, "appid");
        given(usageLuaExecutor.execute(any(UsageLuaExecutor.UsageLuaCommand.class), eq(eventId)))
                .willReturn(
                        new UsageUpdateResult(
                                5000L, 5000L, "NORMAL", 1000L, 0.1, 10000L, false, false));
        given(usageProcessingDecisionMapper.fromLuaStatus("NORMAL")).willReturn(decision);
        given(usageEventOutboxService.findPendingDispatchByEventId(eventId))
                .willReturn(Optional.empty());

        usageSyncServiceImpl.syncUsage(eventId, eventTime, payload);

        verify(usagePersistService).persistFromUsageEvent(eventId, eventTime, payload, "ALLOWED");
        verify(usageNotificationPayloadMapper, never())
                .toNotificationPayload(any(), any(), any(), any());
        verify(usageEventOutboxService, never()).stageAfterRedisApplied(any(), any(), anyBoolean());
    }

    @Test
    @DisplayName("알 수 없는 Lua status면 즉시 실패한다")
    void syncUsage_UnknownLuaStatusThrows() {
        String eventId = "evt_unknown";
        String eventTime = "2026-03-04T12:34:56";
        LocalDate eventMonth = LocalDate.of(2026, 3, 1);
        UsagePayload payload = new UsagePayload(100L, 1L, "appId", 1024L, Map.of());

        stubCommon(100L, 1L, eventMonth, eventId, "appid");
        given(usageLuaExecutor.execute(any(UsageLuaExecutor.UsageLuaCommand.class), eq(eventId)))
                .willReturn(
                        new UsageUpdateResult(
                                5000L, 5000L, "NEW_STATUS", 1000L, 0.1, 10000L, true, false));
        given(usageProcessingDecisionMapper.fromLuaStatus("NEW_STATUS"))
                .willThrow(
                        new NonRetryableKafkaMessageProcessingException(
                                "Unsupported Lua status: NEW_STATUS"));

        assertThrows(
                NonRetryableKafkaMessageProcessingException.class,
                () -> usageSyncServiceImpl.syncUsage(eventId, eventTime, payload));

        verify(usagePersistService, never()).persistFromUsageEvent(any(), any(), any(), any());
    }

    @Test
    @DisplayName("warmup 실패 시 Lua를 실행하지 않고 예외를 던진다")
    void syncUsage_WarmupFailedThrows() {
        String eventId = "evt_2";
        String eventTime = "2026-03-04T10:10:10";
        LocalDate eventMonth = LocalDate.of(2026, 3, 1);
        UsagePayload payload = new UsagePayload(100L, 1L, "appId", 1024L, Map.of());

        stubCommonFailure(100L, 1L, eventMonth, eventId, "appid");

        assertThrows(
                KafkaMessageProcessingException.class,
                () -> usageSyncServiceImpl.syncUsage(eventId, eventTime, payload));

        verify(usageLuaExecutor, never()).execute(any(), any());
    }

    private void stubCommon(
            long familyId, long customerId, LocalDate eventMonth, String eventId, String appId) {
        given(usageFamilyMembershipCacheHelper.isValidFamilyCustomer(familyId, customerId))
                .willReturn(true);
        given(redisKeyGenerator.generateFamilyInfoKey(familyId, eventMonth))
                .willReturn("family:" + familyId + ":info:202603");
        given(redisKeyGenerator.generateFamilyRemainingKey(familyId, eventMonth))
                .willReturn("family:" + familyId + ":remaining:202603");
        given(
                        redisKeyGenerator.generateFamilyCustomerMonthlyUsageKey(
                                familyId, customerId, eventMonth))
                .willReturn("monthlyKey");
        given(redisKeyGenerator.generateFamilyCustomerConstraintsKey(familyId, customerId))
                .willReturn("constraintsKey");
        given(
                        redisKeyGenerator.generateFamilyCustomerThresholdAlertKey(
                                familyId, customerId, 50, eventMonth))
                .willReturn(
                        "family:"
                                + familyId
                                + ":customer:"
                                + customerId
                                + ":alert:THRESHOLD:50:202603");
        given(
                        redisKeyGenerator.generateFamilyCustomerThresholdAlertKey(
                                familyId, customerId, 30, eventMonth))
                .willReturn(
                        "family:"
                                + familyId
                                + ":customer:"
                                + customerId
                                + ":alert:THRESHOLD:30:202603");
        given(
                        redisKeyGenerator.generateFamilyCustomerThresholdAlertKey(
                                familyId, customerId, 10, eventMonth))
                .willReturn(
                        "family:"
                                + familyId
                                + ":customer:"
                                + customerId
                                + ":alert:THRESHOLD:10:202603");
        given(
                        redisKeyGenerator.generateFamilyCustomerBlockAlertKey(
                                familyId, customerId, "MANUAL", eventMonth))
                .willReturn(
                        "family:" + familyId + ":customer:" + customerId + ":alert:MANUAL:202603");
        given(
                        redisKeyGenerator.generateFamilyCustomerAppBlockAlertKey(
                                familyId, customerId, appId, eventMonth))
                .willReturn(
                        "family:"
                                + familyId
                                + ":customer:"
                                + customerId
                                + ":alert:APP_BLOCK:"
                                + appId
                                + ":202603");
        given(
                        redisKeyGenerator.generateFamilyCustomerBlockAlertKey(
                                familyId, customerId, "TIME_BLOCK", eventMonth))
                .willReturn(
                        "family:"
                                + familyId
                                + ":customer:"
                                + customerId
                                + ":alert:TIME_BLOCK:202603");
        given(
                        redisKeyGenerator.generateFamilyCustomerBlockAlertKey(
                                familyId, customerId, "MONTHLY_LIMIT_EXCEEDED", eventMonth))
                .willReturn(
                        "family:"
                                + familyId
                                + ":customer:"
                                + customerId
                                + ":alert:MONTHLY_LIMIT_EXCEEDED:202603");
        given(
                        redisKeyGenerator.generateFamilyCustomerBlockAlertKey(
                                familyId, customerId, "FAMILY_QUOTA_EXCEEDED", eventMonth))
                .willReturn(
                        "family:"
                                + familyId
                                + ":customer:"
                                + customerId
                                + ":alert:FAMILY_QUOTA_EXCEEDED:202603");
        given(redisKeyGenerator.generateUsageEventDedupKey(eventId))
                .willReturn("event:dedup:usage:" + eventId);
        given(
                        usageRedisWarmupHelper.ensureFamilyInfoCached(
                                familyId, eventMonth, "family:" + familyId + ":info:202603"))
                .willReturn(true);
        given(
                        usageRedisWarmupHelper.ensureRemainingBytesCached(
                                familyId, eventMonth, "family:" + familyId + ":remaining:202603"))
                .willReturn(true);
        given(
                        usageRedisWarmupHelper.ensureCustomerUsageCached(
                                familyId, customerId, "monthlyKey", eventMonth))
                .willReturn(true);
    }

    private void stubCommonFailure(
            long familyId, long customerId, LocalDate eventMonth, String eventId, String appId) {
        stubCommon(familyId, customerId, eventMonth, eventId, appId);
        given(
                        usageRedisWarmupHelper.ensureFamilyInfoCached(
                                familyId, eventMonth, "family:" + familyId + ":info:202603"))
                .willReturn(false);
    }
}
