package com.project.domain.family.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.project.domain.family.entity.Family;

public interface FamilyRepository extends JpaRepository<Family, Long> {}
