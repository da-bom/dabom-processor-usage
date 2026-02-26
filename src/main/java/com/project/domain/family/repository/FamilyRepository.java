package com.project.domain.family.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.project.domain.family.entity.Family;

public interface FamilyRepository extends JpaRepository<Family, Long> {

    @Modifying
    @Query(
            "update Family f "
                    + "set f.usedBytes = f.usedBytes + :bytesUsed, "
                    + "f.updatedAt = CURRENT_TIMESTAMP "
                    + "where f.id = :familyId "
                    + "and f.deletedAt is null")
    int updateUsedBytes(@Param("familyId") Long familyId, @Param("bytesUsed") Long bytesUsed);
}
