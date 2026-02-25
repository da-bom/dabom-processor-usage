package com.project.domain.family.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.project.domain.family.entity.FamilyMember;

public interface FamilyMemberRepository extends JpaRepository<FamilyMember, Long> {
    interface FamilyMemberTargetProjection {
        Long getFamilyId();

        Long getCustomerId();
    }

    @Query(
            "select f.familyId as familyId, f.customerId as customerId "
                    + "from FamilyMember f where f.familyId = :familyId and f.deletedAt is null")
    List<FamilyMemberTargetProjection> findAllActiveTargetsByFamilyId(Long familyId);

    @Query(
            "select f.familyId as familyId, f.customerId as customerId "
                    + "from FamilyMember f where f.deletedAt is null")
    List<FamilyMemberTargetProjection> findAllActiveTargets();

    boolean existsByFamilyIdAndCustomerIdAndDeletedAtIsNull(Long familyId, Long customerId);
}
