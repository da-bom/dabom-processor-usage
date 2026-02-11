package com.project.example.infra.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** Example JPA Entity (기술 구현체) - 실제 DB 테이블과 1:1 매핑 - Domain 로직 포함 금지 (순수 데이터 저장 용도) */
@Entity
@Table(name = "EXAMPLE")
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ExampleJpaEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long exampleId;

    private String exampleName;

    private String exampleContent;
}
