package com.project.example.application.repository;

import com.project.example.core.Example;

/** Example 쓰기 전용 저장소 (Port Interface) - 데이터 상태 변경(CUD) 담당 - Domain Layer가 의존하는 인터페이스 (DIP 적용) */
public interface ExampleCommandRepository {
    Example save(Example example);

    void delete(Long exampleId);
}
