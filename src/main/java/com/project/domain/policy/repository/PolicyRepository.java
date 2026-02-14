package com.project.domain.policy.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.project.domain.policy.entity.Policy;

public interface PolicyRepository extends JpaRepository<Policy, Long> {}
