package com.project.domain.customer.entity;

import java.time.LocalDate;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

import com.project.global.util.BaseEntity;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(
        name = "customer_quota",
        uniqueConstraints = {
            @UniqueConstraint(
                    name = "uk_customer_quota_family_customer_month",
                    columnNames = {"family_id", "customer_id", "current_month", "deleted_at"})
        })
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class CustomerQuota extends BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "customer_id", nullable = false)
    private Long customerId;

    @Column(name = "family_id", nullable = false)
    private Long familyId;

    @Column(name = "monthly_limit_bytes")
    private Long monthlyLimitBytes;

    @Column(name = "monthly_used_bytes", nullable = false)
    private Long monthlyUsedBytes;

    @Column(name = "current_month", nullable = false)
    private LocalDate currentMonth;

    @Column(name = "is_blocked", nullable = false)
    private boolean isBlocked;

    @Column(name = "block_reason", length = 50)
    private String blockReason;

    @Builder
    public CustomerQuota(
            Long customerId,
            Long familyId,
            Long monthlyLimitBytes,
            Long monthlyUsedBytes,
            LocalDate currentMonth,
            boolean isBlocked,
            String blockReason) {
        this.customerId = customerId;
        this.familyId = familyId;
        this.monthlyLimitBytes = monthlyLimitBytes;
        this.monthlyUsedBytes = monthlyUsedBytes;
        this.currentMonth = currentMonth;
        this.isBlocked = isBlocked;
        this.blockReason = blockReason;
    }
}
