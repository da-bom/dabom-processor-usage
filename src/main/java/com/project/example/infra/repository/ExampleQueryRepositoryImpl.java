package com.project.example.infra.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.stereotype.Repository;

import com.project.example.application.repository.ExampleQueryRepository;
import com.project.example.core.Example;
import com.project.example.infra.mapper.ExampleEntityMapper;

import lombok.RequiredArgsConstructor;

/** Example 읽기 저장소 구현체 (Adapter) - JPA Repository를 사용하여 DB 조회 수행 - Entity -> Domain 변환하여 반환 */
@Repository
@RequiredArgsConstructor
public class ExampleQueryRepositoryImpl implements ExampleQueryRepository {

    private final JpaExampleRepository jpaExampleRepository;
    private final ExampleEntityMapper exampleEntityMapper;

    @Override
    public Optional<Example> findById(Long exampleId) {
        return jpaExampleRepository.findById(exampleId).map(exampleEntityMapper::toDomain);
    }

    public List<Example> findAll() {
        return jpaExampleRepository.findAll().stream().map(exampleEntityMapper::toDomain).toList();
    }
}
