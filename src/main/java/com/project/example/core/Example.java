package com.project.example.core;

import lombok.Builder;
import lombok.Getter;

/**
 * Example 도메인 (Aggregate Root) - 비즈니스 로직의 핵심 (상태 + 행위) - 순수 POJO (JPA 등 기술 의존성 없음) - 불변 객체
 * (Immutable)
 */
@Getter
@Builder
public class Example {

    private final Long exampleId;
    private final String exampleName;
    private final String exampleContent;

    /**
     * 새로운 Example 도메인 객체 생성
     *
     * @param exampleName 예제 이름
     * @param exampleContent 예제 내용
     * @return 생성된 Example 객체
     */
    public static Example create(String exampleName, String exampleContent) {
        // 비즈니스 규칙 검증
        ExampleRule.validateExampleName(exampleName);
        ExampleRule.validateExampleContent(exampleContent);

        return Example.builder().exampleName(exampleName).exampleContent(exampleContent).build();
    }

    /**
     * Example 정보 업데이트
     *
     * @param exampleName 새로운 이름
     * @param exampleContent 새로운 내용
     * @return 업데이트된 Example 객체 (불변성 유지)
     */
    public Example update(String exampleName, String exampleContent) {
        ExampleRule.validateExampleName(exampleName);
        ExampleRule.validateExampleContent(exampleContent);

        return Example.builder()
                .exampleId(this.exampleId)
                .exampleName(exampleName)
                .exampleContent(exampleContent)
                .build();
    }

    /** ID를 포함한 Example 객체 생성 (Repository에서 조회 시 사용) */
    public static Example withId(Long exampleId, String exampleName, String exampleContent) {
        return Example.builder()
                .exampleId(exampleId)
                .exampleName(exampleName)
                .exampleContent(exampleContent)
                .build();
    }
}
