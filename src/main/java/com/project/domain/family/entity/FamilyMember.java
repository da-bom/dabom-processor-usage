package com.project.domain.family.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import com.project.common.util.BaseEntity;
import com.project.domain.customer.enums.RoleType;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "family_member")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class FamilyMember extends BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "family_id", nullable = false)
    private Long familyId;

    @Column(name = "customer_id", nullable = false)
    private Long customerId;

    @Enumerated(EnumType.STRING)
    @Column(name = "role", nullable = false)
    private RoleType role;

    @Builder
    public FamilyMember(Long id, Long familyId, Long customerId, RoleType role) {
        this.id = id;
        this.familyId = familyId;
        this.customerId = customerId;
        this.role = role;
    }

    public boolean isOwner() {
        return RoleType.OWNER.equals(this.role);
    }
}
