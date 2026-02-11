package com.project.example.web.dto.request;

/**
 * Example 업데이트 요청 DTO
 *
 * @param exampleName 업데이트할 이름
 * @param exampleContent 업데이트할 내용
 */
public record UpdateExampleRequest(String exampleName, String exampleContent) {}
