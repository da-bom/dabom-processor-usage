package com.project.example.web.dto.response;

/**
 * Example 기본 응답 DTO - 필수 정보만 포함
 *
 * @param exampleId Example ID
 * @param exampleName Example 이름
 */
public record ExampleResponse(Long exampleId, String exampleName) {}
