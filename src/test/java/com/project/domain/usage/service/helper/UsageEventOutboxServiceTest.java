package com.project.domain.usage.service.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

import java.time.LocalDateTime;
import java.util.List;
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
import com.project.global.common.TimeConstants;

@ExtendWith(MockitoExtension.class)
class UsageEventOutboxServiceTest {

    @InjectMocks private UsageEventOutboxService usageEventOutboxService;
    @Mock private UsageEventOutboxRepository usageEventOutboxRepository;
    @Spy private ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("dispatch 후보 조회는 PUBLISH_PENDING만 포함한다")
    void findDispatchCandidatesByEventId_FiltersPendingOnly() {
        UsageEventOutbox pending = row("evt_1", UsageOutboxStatus.PUBLISH_PENDING, null);
        UsageEventOutbox failed =
                row("evt_1", UsageOutboxStatus.FAILED, LocalDateTime.now(TimeConstants.ASIA_SEOUL));

        given(usageEventOutboxRepository.findByEventIdOrderByIdAsc("evt_1"))
                .willReturn(List.of(pending, failed));

        List<UsageEventOutbox> candidates =
                usageEventOutboxService.findDispatchCandidatesByEventId("evt_1");

        assertEquals(1, candidates.size());
        assertTrue(candidates.contains(pending));
        assertFalse(candidates.contains(failed));
    }

    @Test
    @DisplayName("notification 대상이면 PREPARED를 PUBLISH_PENDING으로 전이한다")
    void stageAfterRedisApplied_ToPublishPending() {
        NotificationPayload payload =
                new NotificationPayload(
                        100L, 1L, NotificationType.THRESHOLD_ALERT, "title", "message", Map.of());
        UsageEventOutbox pending =
                UsageEventOutbox.builder()
                        .eventId("evt_2")
                        .familyId(100L)
                        .customerId(1L)
                        .status(UsageOutboxStatus.PUBLISH_PENDING)
                        .payloadJson(objectMapper.valueToTree(payload).toString())
                        .retryCount(0)
                        .build();
        given(
                        usageEventOutboxRepository.markPublishPendingIfPrepared(
                                eq("evt_2"), any(String.class)))
                .willReturn(1);
        given(usageEventOutboxRepository.findByEventId("evt_2")).willReturn(Optional.of(pending));

        Optional<UsageEventOutboxService.PendingNotificationDispatch> dispatch =
                usageEventOutboxService.stageAfterRedisApplied("evt_2", payload, true);

        assertTrue(dispatch.isPresent());
        assertEquals(100L, dispatch.get().payload().familyId());
    }

    @Test
    @DisplayName("notification 비대상이면 PREPARED를 SKIPPED로 전이한다")
    void stageAfterRedisApplied_ToSkipped() {
        NotificationPayload payload =
                new NotificationPayload(
                        100L, 1L, NotificationType.THRESHOLD_ALERT, "title", "message", Map.of());
        given(usageEventOutboxRepository.markSkippedIfPrepared("evt_3")).willReturn(1);

        Optional<UsageEventOutboxService.PendingNotificationDispatch> dispatch =
                usageEventOutboxService.stageAfterRedisApplied("evt_3", payload, false);

        assertTrue(dispatch.isEmpty());
        verify(usageEventOutboxRepository).markSkippedIfPrepared("evt_3");
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
        assertEquals(NotificationType.THRESHOLD_ALERT, found.get().payload().type());
    }

    @Test
    @DisplayName("hasPreparedRows는 PREPARED 존재 여부를 repository에 위임한다")
    void hasPreparedRows_DelegatesRepository() {
        given(
                        usageEventOutboxRepository.existsByEventIdAndStatus(
                                "evt_5", UsageOutboxStatus.PREPARED))
                .willReturn(true);

        boolean result = usageEventOutboxService.hasPreparedRows("evt_5");

        assertTrue(result);
        verify(usageEventOutboxRepository)
                .existsByEventIdAndStatus("evt_5", UsageOutboxStatus.PREPARED);
    }

    private UsageEventOutbox row(
            String eventId, UsageOutboxStatus status, LocalDateTime nextRetryAt) {
        return UsageEventOutbox.builder()
                .eventId(eventId)
                .familyId(100L)
                .customerId(1L)
                .status(status)
                .payloadJson("{}")
                .retryCount(status == UsageOutboxStatus.FAILED ? 1 : 0)
                .nextRetryAt(nextRetryAt)
                .build();
    }
}
