package com.project.domain.policy.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.project.domain.policy.entity.PolicyAssignment;
import com.project.domain.policy.enums.PolicyType;

public interface PolicyAssignmentRepository extends JpaRepository<PolicyAssignment, Long> {
    @Query(
            "SELECT p FROM PolicyAssignment p WHERE p.familyId = :familyId AND p.deletedAt"
                    + " IS NULL")
    List<PolicyAssignment> findAllByFamilyId(@Param("familyId") Long familyId);

    @Query(
            "SELECT pa FROM PolicyAssignment pa "
                    + "JOIN Policy p ON pa.policyId = p.id "
                    + "WHERE pa.familyId = :familyId "
                    + "AND pa.targetCustomerId = :targetCustomerId "
                    + "AND p.policyType = :type "
                    + "AND pa.deletedAt IS NULL")
    Optional<PolicyAssignment> findByTargetAndType(
            @Param("familyId") Long familyId,
            @Param("targetCustomerId") Long targetCustomerId,
            @Param("type") PolicyType type);
}
