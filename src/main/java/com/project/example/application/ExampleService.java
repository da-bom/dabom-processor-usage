package com.project.example.application;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.project.example.application.repository.ExampleCommandRepository;
import com.project.example.application.repository.ExampleQueryRepository;
import com.project.example.core.Example;
import com.project.example.core.event.ExampleEventPublisher;
import com.project.example.infra.cache.ExampleCacheRepository;

import lombok.RequiredArgsConstructor;

/**
 * Example Application Service (UseCase) - 트랜잭션 단위 및 비즈니스 흐름 제어 - 도메인 로직(Example.java)에 위임 (순수 서비스)
 */
@Service
@RequiredArgsConstructor
public class ExampleService {

    private final ExampleCommandRepository exampleCommandRepository;
    private final ExampleQueryRepository exampleQueryRepository;
    private final ExampleEventPublisher exampleEventPublisher;
    private final ExampleCacheRepository exampleCacheRepository;

    /** ID로 Example 조회 - Cache Look Aside 패턴 적용 */
    @Transactional(readOnly = true)
    public Example findById(Long exampleId) {
        // 1. Cache 조회
        return exampleCacheRepository
                .findById(exampleId)
                .orElseGet(
                        () -> {
                            // 2. DB 조회
                            Example example =
                                    exampleQueryRepository
                                            .findById(exampleId)
                                            .orElseThrow(
                                                    () ->
                                                            new IllegalArgumentException(
                                                                    "Example not found: "
                                                                            + exampleId));
                            // 3. Cache 저장
                            exampleCacheRepository.save(example);
                            return example;
                        });
    }

    /** Example 생성 - DB 저장 -> 이벤트 발행 -> 캐시 저장 */
    @Transactional
    public Example create(Example example) {
        // 1. DB 저장 (Command)
        Example savedExample = exampleCommandRepository.save(example);

        // 2. Event 발행 (Side Effect)
        exampleEventPublisher.publishExampleCreated(savedExample);

        // 3. Cache 저장 (Consistency)
        exampleCacheRepository.save(savedExample);

        return savedExample;
    }

    /** Example 업데이트 - 검증 -> DB 저장 -> 캐시 갱신 */
    @Transactional
    public Example update(Example example) {
        // 1. 기존 데이터 확인
        findById(example.getExampleId());

        // 2. DB 저장 (Command)
        Example updatedExample = exampleCommandRepository.save(example);

        // 3. Cache 갱신
        exampleCacheRepository.save(updatedExample);

        return updatedExample;
    }
}
