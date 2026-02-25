package com.project.domain.usage.service.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.project.domain.notification.infra.messaging.NotificationKafkaProducer;
import com.project.domain.usage.infra.messaging.UsagePersistKafkaProducer;
import com.project.domain.usage.infra.messaging.UsageRealtimeKafkaProducer;
import com.project.domain.usage.service.dto.UsageUpdateResult;
import com.project.global.event.dto.notification.CustomerBlockedPayload;
import com.project.global.event.dto.notification.ThresholdAlertPayload;
import com.project.global.event.dto.usage.UsagePayload;
import com.project.global.event.dto.usage.UsagePersistPayload;

@ExtendWith(MockitoExtension.class)
class UsageEventPublisherTest {

    @InjectMocks private UsageEventPublisher usageEventPublisher;

    @Mock private UsagePersistKafkaProducer persistProducer;
    @Mock private UsageRealtimeKafkaProducer realtimeProducer;
    @Mock private NotificationKafkaProducer notificationProducer;

    @Test
    @DisplayName("NORMAL 상태면 persist는 ALLOWED로 발행하고 차단/임계 알림은 발행하지 않는다")
    void publish_Normal() {
        UsagePayload payload = new UsagePayload(100L, 1L, "app", 1024L, Map.of());
        UsageUpdateResult result =
                new UsageUpdateResult(5000L, 5000L, "NORMAL", 1000L, 0.1, 10000L);
        UsageEventPublisher.UsageEventContext ctx =
                new UsageEventPublisher.UsageEventContext(
                        "evt_1", "2026-02-24T12:00:00", payload, result);

        usageEventPublisher.publish(ctx);

        ArgumentCaptor<UsagePersistPayload> persistCaptor =
                ArgumentCaptor.forClass(UsagePersistPayload.class);
        verify(persistProducer, times(1)).publish(persistCaptor.capture());
        verify(realtimeProducer, times(1)).publish(any());
        verify(notificationProducer, never()).publish(any(ThresholdAlertPayload.class));
        verify(notificationProducer, never()).publish(any(CustomerBlockedPayload.class));

        assertEquals("ALLOWED", persistCaptor.getValue().processResult());
    }

    @Test
    @DisplayName("WARNING 상태면 임계치 알림을 발행한다")
    void publish_Warning() {
        UsagePayload payload = new UsagePayload(100L, 1L, "app", 1024L, Map.of());
        UsageUpdateResult result =
                new UsageUpdateResult(9000L, 1000L, "WARNING_10", 2000L, 0.2, 10000L);
        UsageEventPublisher.UsageEventContext ctx =
                new UsageEventPublisher.UsageEventContext(
                        "evt_2", "2026-02-24T12:00:00", payload, result);

        usageEventPublisher.publish(ctx);

        verify(notificationProducer, times(1)).publish(any(ThresholdAlertPayload.class));
    }

    @Test
    @DisplayName("차단 상태면 차단 알림을 발행한다")
    void publish_Blocked() {
        UsagePayload payload = new UsagePayload(100L, 1L, "app", 1024L, Map.of());
        UsageUpdateResult result =
                new UsageUpdateResult(8000L, 2000L, "MONTHLY_LIMIT_EXCEEDED", 10001L, 1.0, 10000L);
        UsageEventPublisher.UsageEventContext ctx =
                new UsageEventPublisher.UsageEventContext(
                        "evt_3", "2026-02-24T12:00:00", payload, result);

        usageEventPublisher.publish(ctx);

        verify(notificationProducer, times(1)).publish(any(CustomerBlockedPayload.class));
    }
}
