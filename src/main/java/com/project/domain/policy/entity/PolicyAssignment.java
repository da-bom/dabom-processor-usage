package com.project.domain.policy.entity;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import com.project.global.util.BaseEntity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "policy_assignment")
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PolicyAssignment extends BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private Long policyId;

    @Column(nullable = false)
    private Long familyId;

    @Column(name = "target_customer_id")
    private Long targetCustomerId;

    @Column(columnDefinition = "json", nullable = false)
    private String rules;

    @Column(nullable = false)
    private boolean isActive;

    private LocalDateTime appliedAt;

    private Long appliedById;

    public void update(String newRules, Boolean isActive, Long actorId) {
        if (newRules != null) {
            this.rules = newRules;
        }
        if (isActive != null) {
            boolean previousActiveState = this.isActive;
            this.isActive = isActive;

            if (isActive && !previousActiveState) {
                this.appliedAt = LocalDateTime.now();
                this.appliedById = actorId;
            }
        }
    }
}
