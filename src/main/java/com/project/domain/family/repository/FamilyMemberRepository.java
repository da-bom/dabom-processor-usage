package com.project.domain.family.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.project.domain.customer.enums.RoleType;
import com.project.domain.family.entity.FamilyMember;

public interface FamilyMemberRepository extends JpaRepository<FamilyMember, Long> {
    List<FamilyMember> findAllByFamilyId(Long familyId);

    @Query("select f.role from FamilyMember f where f.customerId = :customerId")
    RoleType findRoleById(Long customerId);
}
