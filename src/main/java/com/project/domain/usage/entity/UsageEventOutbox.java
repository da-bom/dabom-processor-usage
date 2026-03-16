package com.project.domain.usage.entity;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

import com.project.domain.usage.enums.UsageOutboxStatus;
import com.project.global.util.BaseEntity;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(
        name = "usage_event_outbox",
        uniqueConstraints = {
            @UniqueConstraint(
                    name = "uk_usage_event_outbox_event_id",
                    columnNames = {"event_id"})
        },
        indexes = {
            @Index(name = "idx_usage_outbox_status_retry", columnList = "status,next_retry_at"),
            @Index(name = "idx_usage_outbox_event_id", columnList = "event_id")
        })
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class UsageEventOutbox extends BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false)
    private String eventId;

    @Column(name = "family_id", nullable = false)
    private Long familyId;

    @Column(name = "customer_id", nullable = false)
    private Long customerId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private UsageOutboxStatus status;

    @Column(name = "payload_json", columnDefinition = "TEXT")
    private String payloadJson;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "next_retry_at")
    private LocalDateTime nextRetryAt;

    @Column(name = "last_error", length = 1000)
    private String lastError;

    @Builder
    public UsageEventOutbox(
            String eventId,
            Long familyId,
            Long customerId,
            UsageOutboxStatus status,
            String payloadJson,
            int retryCount,
            LocalDateTime nextRetryAt,
            String lastError) {
        this.eventId = eventId;
        this.familyId = familyId;
        this.customerId = customerId;
        this.status = status;
        this.payloadJson = payloadJson;
        this.retryCount = retryCount;
        this.nextRetryAt = nextRetryAt;
        this.lastError = lastError;
    }

    // 최초 복구 기준점 상태를 만든다.
    public static UsageEventOutbox prepared(String eventId, Long familyId, Long customerId) {
        return UsageEventOutbox.builder()
                .eventId(eventId)
                .familyId(familyId)
                .customerId(customerId)
                .status(UsageOutboxStatus.PREPARED)
                .retryCount(0)
                .build();
    }

    // 즉시 발행 또는 배치 복구 대기 상태로 전이한다.
    public void markPublishPending(String payloadJson) {
        this.status = UsageOutboxStatus.PUBLISH_PENDING;
        this.payloadJson = payloadJson;
        this.nextRetryAt = null;
        this.lastError = null;
    }

    // 알림 비대상으로 종료한다.
    public void markSkipped() {
        this.status = UsageOutboxStatus.SKIPPED;
        this.payloadJson = null;
        this.nextRetryAt = null;
        this.lastError = null;
    }

    // 발행 성공으로 종료한다.
    public void markSent() {
        this.status = UsageOutboxStatus.SENT;
        this.nextRetryAt = null;
        this.lastError = null;
    }

    // 배치 서버가 최종 실패로 확정한다.
    public void markFailed(String message, LocalDateTime nextRetryAt) {
        this.status = UsageOutboxStatus.FAILED;
        this.retryCount += 1;
        this.nextRetryAt = nextRetryAt;
        this.lastError = message;
    }
}
