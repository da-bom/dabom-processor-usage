package com.project.global.exception.code;

import org.springframework.http.HttpStatus;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum ExampleErrorCode implements BaseErrorCode {
    EXAMPLE_NOT_FOUND(HttpStatus.BAD_REQUEST, "EXAMPLE_001", "Example을 찾을 수 없습니다");

    private final HttpStatus httpStatus;
    private final String customCode;
    private final String message;
}
