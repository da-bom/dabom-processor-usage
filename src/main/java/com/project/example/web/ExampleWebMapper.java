package com.project.example.web;

import org.springframework.stereotype.Component;

import com.project.example.core.Example;
import com.project.example.web.dto.request.CreateExampleRequest;
import com.project.example.web.dto.request.UpdateExampleRequest;
import com.project.example.web.dto.response.ExampleDetailResponse;
import com.project.example.web.dto.response.ExampleResponse;

/** Web 계층 Mapper - DTO ↔ Domain 변환 전담 - Controller와 Domain 사이의 통역사 역할 */
@Component
public class ExampleWebMapper {

    /**
     * CreateExampleRequest → Example Domain 변환
     *
     * @param request 생성 요청 DTO
     * @return Example 도메인 객체
     */
    public Example toDomain(CreateExampleRequest request) {
        return Example.create(request.exampleName(), request.exampleContent());
    }

    /**
     * UpdateExampleRequest → Example Domain 변환
     *
     * @param exampleId 업데이트할 Example ID
     * @param request 업데이트 요청 DTO
     * @return Example 도메인 객체
     */
    public Example toDomain(Long exampleId, UpdateExampleRequest request) {
        return Example.withId(exampleId, request.exampleName(), request.exampleContent());
    }

    /**
     * Example Domain → ExampleResponse 변환
     *
     * @param example Example 도메인 객체
     * @return 기본 응답 DTO
     */
    public ExampleResponse toResponse(Example example) {
        return new ExampleResponse(example.getExampleId(), example.getExampleName());
    }

    /**
     * Example Domain → ExampleDetailResponse 변환
     *
     * @param example Example 도메인 객체
     * @return 상세 응답 DTO
     */
    public ExampleDetailResponse toDetailResponse(Example example) {
        return new ExampleDetailResponse(
                example.getExampleId(), example.getExampleName(), example.getExampleContent());
    }
}
