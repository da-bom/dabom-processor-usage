package com.project.domain.usage.entity;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

import com.project.common.util.BaseEntity;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(
        name = "usage_record",
        uniqueConstraints = {
            @UniqueConstraint(
                    name = "uk_usage_record_event_id",
                    columnNames = {"event_id"})
        },
        indexes = {
            @Index(name = "idx_usage_family_time", columnList = "family_id,event_time"),
            @Index(name = "idx_usage_customer_time", columnList = "customer_id,event_time"),
            @Index(name = "idx_usage_event_id", columnList = "event_id")
        })
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class UsageRecord extends BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false)
    private String eventId;

    @Column(name = "customer_id", nullable = false)
    private Long customerId;

    @Column(name = "family_id", nullable = false)
    private Long familyId;

    @Column(name = "bytes_used", nullable = false)
    private Long bytesUsed;

    @Column(name = "app_id")
    private String appId;

    @Column(name = "event_time", nullable = false)
    private LocalDateTime eventTime;

    @Builder
    public UsageRecord(
            String eventId,
            Long customerId,
            Long familyId,
            Long bytesUsed,
            String appId,
            LocalDateTime eventTime) {
        this.eventId = eventId;
        this.customerId = customerId;
        this.familyId = familyId;
        this.bytesUsed = bytesUsed;
        this.appId = appId;
        this.eventTime = eventTime;
    }
}
