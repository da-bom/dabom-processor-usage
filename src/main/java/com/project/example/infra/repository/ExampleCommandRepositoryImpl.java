package com.project.example.infra.repository;

import org.springframework.stereotype.Repository;

import com.project.example.application.repository.ExampleCommandRepository;
import com.project.example.core.Example;
import com.project.example.infra.entity.ExampleJpaEntity;
import com.project.example.infra.mapper.ExampleEntityMapper;

import lombok.RequiredArgsConstructor;

/**
 * Example Command Repository 구현체 (Adapter) - 쓰기 작업 처리 (생성, 수정, 삭제) - Domain 객체를 받아 JPA Entity로 변환 후
 * 저장 - EntityMapper를 통한 Domain ↔ Entity 변환
 */
@Repository
@RequiredArgsConstructor
public class ExampleCommandRepositoryImpl implements ExampleCommandRepository {

    private final JpaExampleRepository jpaExampleRepository;
    private final ExampleEntityMapper exampleEntityMapper;

    /**
     * Example 저장 (생성 또는 업데이트)
     *
     * @param example Example 도메인 객체
     * @return 저장된 Example 도메인 객체
     */
    @Override
    public Example save(Example example) {
        ExampleJpaEntity entity = exampleEntityMapper.toEntity(example);
        ExampleJpaEntity savedEntity = jpaExampleRepository.save(entity);
        return exampleEntityMapper.toDomain(savedEntity);
    }

    /**
     * Example 삭제
     *
     * @param exampleId 삭제할 Example ID
     */
    @Override
    public void delete(Long exampleId) {
        jpaExampleRepository.deleteById(exampleId);
    }
}
