package com.project.common.exception.code;

import org.springframework.http.HttpStatus;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum PolicyErrorCode implements BaseErrorCode {
    POLICY_NOT_FOUND(HttpStatus.NOT_FOUND, "POLICY_001", "정책을 찾을 수 없습니다"),
    POLICY_ASSIGNMENT_NOT_FOUND(HttpStatus.NOT_FOUND, "POLICY_002", "해당 정책 할당을 찾을 수 없습니다"),
    POLICY_NOT_MODIFIABLE(HttpStatus.BAD_REQUEST, "POLICY_003", "수정할 수 없는 정책입니다"),
    POLICY_REDIS_SYNC_FAILED(
            HttpStatus.INTERNAL_SERVER_ERROR, "POLICY_004", "정책 변경사항을 레디스에 반영하지 못했습니다."),
    POLICY_REDIS_INVALID_RESULT(
            HttpStatus.INTERNAL_SERVER_ERROR, "POLICY_005", "유효하지 않은 Redis Lua 스크립트 결과입니다.");

    private final HttpStatus httpStatus;
    private final String customCode;
    private final String message;
}
