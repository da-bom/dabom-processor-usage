package com.project.example.web.dto.response;

/**
 * Example 상세 응답 DTO - 모든 정보 포함
 *
 * @param exampleId Example ID
 * @param exampleName Example 이름
 * @param exampleContent Example 내용
 */
public record ExampleDetailResponse(Long exampleId, String exampleName, String exampleContent) {}
