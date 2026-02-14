package com.project.domain.policy.entity;

import java.util.Map;

import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import com.project.domain.customer.enums.RoleType;
import com.project.domain.policy.enums.PolicyType;
import com.project.global.util.BaseEntity;
import com.project.global.util.MapStringObjectConverter;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "policy")
@Getter
@Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
public class Policy extends BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String name;

    private String description;

    @Enumerated(EnumType.STRING)
    @Column(name = "require_role")
    private RoleType requiredRole;

    @Enumerated(EnumType.STRING)
    @Column(name = "type", nullable = false)
    private PolicyType policyType;

    @Column(name = "default_rules", columnDefinition = "json")
    @Convert(converter = MapStringObjectConverter.class)
    private Map<String, Object> defaultRules;

    @Column(name = "is_system", columnDefinition = "tinyint", nullable = false)
    private boolean isSystem;

    @Column(name = "is_active")
    private boolean isActive;

    public boolean isModifiable() {
        return !isSystem && !isDeleted();
    }

    public void update(
            String description,
            RoleType requiredRole,
            PolicyType policyType,
            Map<String, Object> defaultRules,
            boolean isActive) {
        this.description = description;
        this.requiredRole = requiredRole;
        this.policyType = policyType;
        this.defaultRules = defaultRules;
        this.isActive = isActive;
    }
}
