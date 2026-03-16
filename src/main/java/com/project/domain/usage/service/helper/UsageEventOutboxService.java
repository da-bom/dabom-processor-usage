package com.project.domain.usage.service.helper;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.dabom.messaging.kafka.error.KafkaMessageProcessingException;
import com.dabom.messaging.kafka.error.NonRetryableKafkaMessageProcessingException;
import com.dabom.messaging.kafka.event.dto.notification.NotificationPayload;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.domain.usage.entity.UsageEventOutbox;
import com.project.domain.usage.enums.UsageOutboxStatus;
import com.project.domain.usage.repository.UsageEventOutboxRepository;
import com.project.global.common.TimeConstants;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class UsageEventOutboxService {

    private static final long[] RETRY_BACKOFF_SECONDS = {10L, 30L, 60L, 300L, 600L};

    private final UsageEventOutboxRepository usageEventOutboxRepository;
    private final ObjectMapper objectMapper;

    // 이벤트 단위 Outbox 기본 행을 준비한다.
    @Transactional
    public void ensurePrepared(String eventId, long familyId, long customerId) {
        usageEventOutboxRepository.insertPreparedIgnore(eventId, familyId, customerId);
    }

    // Redis 처리 결과를 바탕으로 notification 발행 대기 상태를 확정한다.
    @Transactional
    public Optional<PendingNotificationDispatch> stageAfterRedisApplied(
            String eventId, NotificationPayload payload, boolean publishNotification) {
        if (!publishNotification) {
            int skipped = usageEventOutboxRepository.markSkippedIfPrepared(eventId);
            if (skipped == 1) {
                return Optional.empty();
            }
            return Optional.empty();
        }

        String payloadJson = toJson(payload);
        int updated = usageEventOutboxRepository.markPublishPendingIfPrepared(eventId, payloadJson);
        if (updated == 1) {
            return findPendingDispatchByEventId(eventId);
        }

        return Optional.empty();
    }

    // eventId 기준으로 아직 발행되지 않은 notification payload를 찾는다.
    @Transactional(readOnly = true)
    public Optional<PendingNotificationDispatch> findPendingDispatchByEventId(String eventId) {
        return usageEventOutboxRepository
                .findByEventId(eventId)
                .filter(row -> row.getStatus() == UsageOutboxStatus.PUBLISH_PENDING)
                .map(
                        row ->
                                new PendingNotificationDispatch(
                                        row.getId(),
                                        fromJson(row.getPayloadJson(), NotificationPayload.class)));
    }

    // 배치 복구 대상 후보를 eventId 기준으로 조회한다.
    @Transactional(readOnly = true)
    public List<UsageEventOutbox> findDispatchCandidatesByEventId(String eventId) {
        return usageEventOutboxRepository.findByEventIdOrderByIdAsc(eventId).stream()
                .filter(row -> row.getStatus() == UsageOutboxStatus.PUBLISH_PENDING)
                .toList();
    }

    // 발행 성공 시 Outbox 상태를 SENT로 변경한다.
    @Transactional
    public void markSent(Long outboxId) {
        int updated = usageEventOutboxRepository.markSentIfPending(outboxId);
        if (updated == 0) {
            log.debug("Skip markSent because outbox is no longer pending. outboxId={}", outboxId);
        }
    }

    // 배치 서버가 최종 실패를 기록한다.
    @Transactional
    public void markFailed(Long outboxId, String reason) {
        UsageEventOutbox row =
                usageEventOutboxRepository
                        .findById(outboxId)
                        .orElseThrow(
                                () ->
                                        new KafkaMessageProcessingException(
                                                "Outbox row not found. id=%d".formatted(outboxId),
                                                new IllegalStateException("Outbox row not found")));

        LocalDateTime nextRetryAt =
                LocalDateTime.now(TimeConstants.ASIA_SEOUL)
                        .plusSeconds(resolveBackoffSeconds(row.getRetryCount() + 1));
        row.markFailed(abbreviateError(reason), nextRetryAt);
    }

    // PREPARED 상태 행이 남아 있는지 확인한다.
    @Transactional(readOnly = true)
    public boolean hasPreparedRows(String eventId) {
        return usageEventOutboxRepository.existsByEventIdAndStatus(
                eventId, UsageOutboxStatus.PREPARED);
    }

    // 저장한 payload_json을 지정한 타입으로 역직렬화한다.
    public <T> T fromJson(String payloadJson, Class<T> clazz) {
        try {
            return objectMapper.readValue(payloadJson, clazz);
        } catch (JsonProcessingException e) {
            throw new NonRetryableKafkaMessageProcessingException(
                    "Failed to deserialize outbox payload", e);
        }
    }

    // Outbox payload를 JSON 문자열로 직렬화한다.
    private String toJson(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new NonRetryableKafkaMessageProcessingException(
                    "Failed to serialize outbox payload", e);
        }
    }

    // 에러 메시지를 저장 가능한 길이로 자른다.
    private String abbreviateError(String reason) {
        if (reason == null) {
            return "unknown";
        }
        return reason.length() > 1000 ? reason.substring(0, 1000) : reason;
    }

    // 재시도 횟수에 따라 backoff 초를 계산한다.
    private long resolveBackoffSeconds(int nextRetryCount) {
        int index = Math.max(0, Math.min(RETRY_BACKOFF_SECONDS.length - 1, nextRetryCount - 1));
        return RETRY_BACKOFF_SECONDS[index];
    }

    // 즉시 발행 또는 배치 발행에 사용하는 pending payload 묶음이다.
    public record PendingNotificationDispatch(Long outboxId, NotificationPayload payload) {}
}
