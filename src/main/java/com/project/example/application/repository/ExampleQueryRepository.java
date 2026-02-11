package com.project.example.application.repository;

import java.util.List;
import java.util.Optional;

import com.project.example.core.Example;

/** Example 읽기 전용 저장소 (Port Interface) - 데이터 조회(R) 담당 - 복잡한 조회 조건이나 DTO 반환 전략 적용 가능 */
public interface ExampleQueryRepository {
    Optional<Example> findById(Long exampleId);

    List<Example> findAll();
}
