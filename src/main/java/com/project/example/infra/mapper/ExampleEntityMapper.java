package com.project.example.infra.mapper;

import org.springframework.stereotype.Component;

import com.project.example.core.Example;
import com.project.example.infra.entity.ExampleJpaEntity;

/** Infra 계층 Mapper - Domain 객체 <-> JPA Entity 변환 - DB 기술(JPA)과 Domain의 격리 담당 */
@Component
public class ExampleEntityMapper {

    /**
     * Example Domain → ExampleJpaEntity 변환
     *
     * @param example Example 도메인 객체
     * @return JPA Entity
     */
    public ExampleJpaEntity toEntity(Example example) {
        return ExampleJpaEntity.builder()
                .exampleId(example.getExampleId())
                .exampleName(example.getExampleName())
                .exampleContent(example.getExampleContent())
                .build();
    }

    /**
     * ExampleJpaEntity → Example Domain 변환
     *
     * @param entity JPA Entity
     * @return Example 도메인 객체
     */
    public Example toDomain(ExampleJpaEntity entity) {
        return Example.withId(
                entity.getExampleId(), entity.getExampleName(), entity.getExampleContent());
    }
}
