package com.project.example.web.dto.request;

/**
 * Example 생성 요청 DTO
 *
 * @param exampleName 생성할 이름
 * @param exampleContent 생성할 내용
 */
public record CreateExampleRequest(String exampleName, String exampleContent) {}
