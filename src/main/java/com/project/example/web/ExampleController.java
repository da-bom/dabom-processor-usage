package com.project.example.web;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.project.example.application.ExampleService;
import com.project.example.core.Example;
import com.project.example.web.dto.request.CreateExampleRequest;
import com.project.example.web.dto.request.UpdateExampleRequest;
import com.project.example.web.dto.response.ExampleDetailResponse;
import com.project.example.web.dto.response.ExampleResponse;
import com.project.global.api.response.ApiResponse;

import lombok.RequiredArgsConstructor;

/**
 * Example Web Adapter (Controller) - HTTP 요청 진입점 (Request/Response 처리) - DTO <-> Domain 변환은 Mapper에
 * 위임 - 비즈니스 로직은 Service에 위임
 */
@RestController
@RequestMapping("/example")
@RequiredArgsConstructor
public class ExampleController {

    private final ExampleService exampleService;
    private final ExampleWebMapper exampleWebMapper;

    /**
     * Example 상세 조회
     *
     * @param exampleId Example ID
     * @return Example 상세 정보
     */
    @GetMapping("/{exampleId}")
    public ApiResponse<ExampleDetailResponse> findById(@PathVariable Long exampleId) {
        Example example = exampleService.findById(exampleId);
        return ApiResponse.success(exampleWebMapper.toDetailResponse(example));
    }

    /**
     * Example 생성
     *
     * @param request 생성 요청 DTO
     * @return 생성된 Example 정보
     */
    @PostMapping
    public ApiResponse<ExampleResponse> create(@RequestBody CreateExampleRequest request) {
        Example example = exampleWebMapper.toDomain(request);
        Example createdExample = exampleService.create(example);
        return ApiResponse.created(exampleWebMapper.toResponse(createdExample));
    }

    /**
     * Example 업데이트
     *
     * @param exampleId Example ID
     * @param request 업데이트 요청 DTO
     * @return 업데이트된 Example 정보
     */
    @PutMapping("/{exampleId}")
    public ApiResponse<ExampleResponse> update(
            @PathVariable Long exampleId, @RequestBody UpdateExampleRequest request) {
        Example example = exampleWebMapper.toDomain(exampleId, request);
        Example updatedExample = exampleService.update(example);
        return ApiResponse.success(exampleWebMapper.toResponse(updatedExample));
    }
}
