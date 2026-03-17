package com.project.domain.usage.service.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import com.dabom.messaging.kafka.event.dto.notification.NotificationPayload;
import com.dabom.messaging.kafka.event.dto.notification.NotificationType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.domain.usage.entity.UsageEventOutbox;
import com.project.domain.usage.enums.UsageOutboxStatus;
import com.project.domain.usage.repository.UsageEventOutboxRepository;

@ExtendWith(MockitoExtension.class)
class UsageEventOutboxServiceTest {

    @InjectMocks private UsageEventOutboxService usageEventOutboxService;
    @Mock private UsageEventOutboxRepository usageEventOutboxRepository;
    @Spy private ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("notification 대상이면 PUBLISH_PENDING row를 보장한다")
    void stageAfterRedisApplied_ToPublishPending() {
        NotificationPayload payload =
                new NotificationPayload(
                        100L, 1L, NotificationType.THRESHOLD_ALERT, "title", "message", Map.of());
        UsageEventOutbox pending =
                UsageEventOutbox.builder()
                        .id(10L)
                        .eventId("evt_2")
                        .familyId(100L)
                        .customerId(1L)
                        .status(UsageOutboxStatus.PUBLISH_PENDING)
                        .payloadJson(objectMapper.valueToTree(payload).toString())
                        .retryCount(0)
                        .build();

        given(
                        usageEventOutboxRepository.insertPublishPendingIgnore(
                                eq("evt_2"), eq(100L), eq(1L), any(String.class)))
                .willReturn(1);
        given(usageEventOutboxRepository.refreshPendingPayload(eq("evt_2"), any(String.class)))
                .willReturn(1);
        given(usageEventOutboxRepository.findByEventId("evt_2")).willReturn(Optional.of(pending));

        Optional<UsageEventOutboxService.PendingNotificationDispatch> dispatch =
                usageEventOutboxService.stageAfterRedisApplied("evt_2", payload, true);

        assertTrue(dispatch.isPresent());
        assertEquals(100L, dispatch.get().payload().familyId());
        verify(usageEventOutboxRepository)
                .insertPublishPendingIgnore(eq("evt_2"), eq(100L), eq(1L), any(String.class));
        verify(usageEventOutboxRepository).refreshPendingPayload(eq("evt_2"), any(String.class));
    }

    @Test
    @DisplayName("notification 비대상이면 outbox row를 만들지 않는다")
    void stageAfterRedisApplied_WhenNotificationSkipped_ReturnsEmpty() {
        NotificationPayload payload =
                new NotificationPayload(
                        100L, 1L, NotificationType.THRESHOLD_ALERT, "title", "message", Map.of());

        Optional<UsageEventOutboxService.PendingNotificationDispatch> dispatch =
                usageEventOutboxService.stageAfterRedisApplied("evt_3", payload, false);

        assertTrue(dispatch.isEmpty());
        verify(usageEventOutboxRepository, never())
                .insertPublishPendingIgnore(any(), any(Long.class), any(Long.class), any());
        verify(usageEventOutboxRepository, never()).refreshPendingPayload(any(), any());
    }

    @Test
    @DisplayName("pending payload는 eventId 기준으로 다시 읽을 수 있다")
    void findPendingDispatchByEventId_ReturnsPayload() {
        NotificationPayload payload =
                new NotificationPayload(
                        100L,
                        1L,
                        NotificationType.THRESHOLD_ALERT,
                        "title",
                        "message",
                        Map.of("threshold", 10));
        UsageEventOutbox pending =
                UsageEventOutbox.builder()
                        .id(20L)
                        .eventId("evt_4")
                        .familyId(100L)
                        .customerId(1L)
                        .status(UsageOutboxStatus.PUBLISH_PENDING)
                        .payloadJson(objectMapper.valueToTree(payload).toString())
                        .retryCount(0)
                        .build();
        given(usageEventOutboxRepository.findByEventId("evt_4")).willReturn(Optional.of(pending));

        Optional<UsageEventOutboxService.PendingNotificationDispatch> found =
                usageEventOutboxService.findPendingDispatchByEventId("evt_4");

        assertTrue(found.isPresent());
        assertEquals(20L, found.get().outboxId());
        assertEquals(NotificationType.THRESHOLD_ALERT, found.get().payload().type());
    }
}
