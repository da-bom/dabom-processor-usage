package com.project.domain.family.entity;

import java.time.LocalDate;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import com.project.global.util.BaseEntity;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "family")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Family extends BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "name", nullable = false, length = 100)
    private String name;

    @Column(name = "created_by_id", nullable = false)
    private Long createdById;

    @Column(name = "total_quota_bytes", nullable = false)
    private Long totalQuotaBytes;

    @Column(name = "used_bytes", nullable = false)
    private Long usedBytes;

    @Column(name = "current_month", nullable = false)
    private LocalDate currentMonth;

    @Builder
    public Family(
            Long id,
            String name,
            Long createdById,
            Long totalQuotaBytes,
            Long usedBytes,
            LocalDate currentMonth) {
        this.id = id;
        this.name = name;
        this.createdById = createdById;
        this.totalQuotaBytes = totalQuotaBytes;
        this.usedBytes = usedBytes;
        this.currentMonth = currentMonth;
    }

    public double calculateUsedPercent() {
        if (totalQuotaBytes == null || totalQuotaBytes == 0) {
            return 0.0;
        }
        return (double) usedBytes / totalQuotaBytes * 100.0;
    }

    public void changeName(String name) {
        this.name = name;
    }
}
