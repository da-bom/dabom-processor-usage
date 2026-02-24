package com.project.domain.family.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.project.domain.customer.enums.RoleType;
import com.project.domain.family.entity.FamilyMember;

public interface FamilyMemberRepository extends JpaRepository<FamilyMember, Long> {
    interface FamilyMemberTargetProjection {
        Long getFamilyId();

        Long getCustomerId();
    }

    List<FamilyMember> findAllByFamilyId(Long familyId);

    List<FamilyMember> findAllByFamilyIdAndDeletedAtIsNull(Long familyId);

    @Query(
            "select f.familyId as familyId, f.customerId as customerId "
                    + "from FamilyMember f where f.deletedAt is null")
    List<FamilyMemberTargetProjection> findAllActiveTargets();

    boolean existsByFamilyIdAndCustomerIdAndDeletedAtIsNull(Long familyId, Long customerId);

    @Query("select f.role from FamilyMember f where f.customerId = :customerId")
    RoleType findRoleById(Long customerId);
}
