package com.project.domain.eventoutbox.entity;

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

import com.project.common.util.BaseEntity;
import com.project.domain.eventoutbox.enums.EventOutboxStatus;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Table(
        name = "usage_event_outbox",
        uniqueConstraints = {
            @UniqueConstraint(name = "uk_usage_event_outbox_event_id", columnNames = "event_id")
        },
        indexes = {
            @Index(name = "idx_usage_outbox_status_retry", columnList = "status, next_retry_at")
        })
public class EventOutbox extends BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, length = 191)
    private String eventId;

    @Column(name = "family_id", nullable = false)
    private Long familyId;

    @Column(name = "customer_id", nullable = false)
    private Long customerId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 30)
    private EventOutboxStatus status;

    @Column(name = "payload_json", columnDefinition = "TEXT")
    private String payloadJson;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "next_retry_at")
    private LocalDateTime nextRetryAt;

    @Column(name = "last_error", length = 1000)
    private String lastError;

    @Builder
    private EventOutbox(
            Long id,
            String eventId,
            Long familyId,
            Long customerId,
            EventOutboxStatus status,
            String payloadJson,
            int retryCount,
            LocalDateTime nextRetryAt,
            String lastError) {
        this.id = id;
        this.eventId = eventId;
        this.familyId = familyId;
        this.customerId = customerId;
        this.status = status;
        this.payloadJson = payloadJson;
        this.retryCount = retryCount;
        this.nextRetryAt = nextRetryAt;
        this.lastError = lastError;
    }
}
