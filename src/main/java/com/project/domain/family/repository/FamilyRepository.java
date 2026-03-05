package com.project.domain.family.repository;

import java.time.LocalDate;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.project.domain.family.entity.Family;

public interface FamilyRepository extends JpaRepository<Family, Long> {

    @Modifying
    @Query(
            "update Family f "
                    + "set f.currentMonth = case when f.currentMonth < :eventMonth then"
                    + " :eventMonth else f.currentMonth end, "
                    + "f.usedBytes = case "
                    + "when f.currentMonth < :eventMonth then :bytesUsed "
                    + "when f.currentMonth = :eventMonth then f.usedBytes + :bytesUsed "
                    + "else f.usedBytes end, "
                    + "f.updatedAt = CURRENT_TIMESTAMP "
                    + "where f.id = :familyId "
                    + "and f.deletedAt is null")
    int updateUsedBytesByEventMonth(
            @Param("familyId") Long familyId,
            @Param("eventMonth") LocalDate eventMonth,
            @Param("bytesUsed") Long bytesUsed);
}
