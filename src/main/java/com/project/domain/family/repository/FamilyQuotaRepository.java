package com.project.domain.family.repository;

import java.time.LocalDate;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.project.domain.family.entity.FamilyQuota;

public interface FamilyQuotaRepository extends JpaRepository<FamilyQuota, Long> {

    @Query(
            """
        SELECT fq
        FROM FamilyQuota fq
        WHERE fq.familyId = :familyId
          AND fq.currentMonth = :currentMonth
          AND fq.deletedAt IS NULL
        """)
    Optional<FamilyQuota> findActiveByFamilyIdAndCurrentMonth(
            @Param("familyId") Long familyId, @Param("currentMonth") LocalDate currentMonth);

    Optional<FamilyQuota> findTopByFamilyIdAndDeletedAtIsNullOrderByCurrentMonthDesc(Long familyId);

    @Query(
            value =
                    """
            SELECT fq.*
            FROM family_quota fq
            WHERE fq.family_id = :familyId
              AND fq.deleted_at IS NULL
            ORDER BY fq.current_month DESC, fq.id DESC
            LIMIT 1
            FOR UPDATE
            """,
            nativeQuery = true)
    Optional<FamilyQuota> findLatestByFamilyIdForUpdate(@Param("familyId") Long familyId);

    @Modifying
    @Query(
            "update FamilyQuota fq "
                    + "set fq.usedBytes = fq.usedBytes + :bytesUsed, "
                    + "fq.updatedAt = CURRENT_TIMESTAMP "
                    + "where fq.familyId = :familyId "
                    + "and fq.currentMonth = :currentMonth "
                    + "and fq.deletedAt is null")
    int incrementUsedBytes(
            @Param("familyId") Long familyId,
            @Param("currentMonth") LocalDate currentMonth,
            @Param("bytesUsed") Long bytesUsed);
}
