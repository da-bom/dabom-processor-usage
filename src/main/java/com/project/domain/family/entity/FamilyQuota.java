package com.project.domain.family.entity;

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
        name = "family_quota",
        uniqueConstraints =
                @UniqueConstraint(
                        name = "uk_family_quota_family_month",
                        columnNames = {"family_id", "current_month", "deleted_at"}))
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class FamilyQuota extends BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "family_id", nullable = false)
    private Long familyId;

    @Column(name = "current_month", nullable = false)
    private LocalDate currentMonth;

    @Column(name = "total_quota_bytes", nullable = false)
    private Long totalQuotaBytes;

    @Column(name = "used_bytes", nullable = false)
    private Long usedBytes;

    @Builder
    public FamilyQuota(
            Long id, Long familyId, LocalDate currentMonth, Long totalQuotaBytes, Long usedBytes) {
        this.id = id;
        this.familyId = familyId;
        this.currentMonth = currentMonth;
        this.totalQuotaBytes = totalQuotaBytes;
        this.usedBytes = usedBytes;
    }
}
