package com.project.example.infra.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.project.example.infra.entity.ExampleJpaEntity;

/** Spring Data JPA Repository (기술 구현체) - 실제 DB 접근 전용 Interface (Hibernate 사용) - 기본적인 CRUD 메서드 제공 */
public interface JpaExampleRepository extends JpaRepository<ExampleJpaEntity, Long> {}
