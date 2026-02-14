package com.project.global.exception.code;

import org.springframework.http.HttpStatus;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum PolicyErrorCode implements BaseErrorCode {
    POLICY_NOT_FOUND(HttpStatus.NOT_FOUND, "POLICY_001", "정책을 찾을 수 없습니다"),
    POLICY_ASSIGNMENT_NOT_FOUND(HttpStatus.NOT_FOUND, "POLICY_002", "해당 정책 할당을 찾을 수 없습니다"),
    POLICY_NOT_MODIFIABLE(HttpStatus.BAD_REQUEST, "POLICY_003", "수정할 수 없는 정책입니다");

    private final HttpStatus httpStatus;
    private final String customCode;
    private final String message;
}
