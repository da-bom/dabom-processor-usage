package com.project.global.exception.code;

import org.springframework.http.HttpStatus;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum FamilyErrorCode implements BaseErrorCode {
    FAMILY_NOT_FOUND(HttpStatus.NOT_FOUND, "FAMILY_001", "가족 정보를 찾을 수 없습니다."),
    FAMILY_INVALID_SEARCH_CONDITION(HttpStatus.BAD_REQUEST, "FAMILY_002", "가족 검색 조건이 올바르지 않습니다."),
    LATEST_QUOTA_SNAPSHOT_NOT_FOUND(
            HttpStatus.NOT_FOUND, "FAMILY_003", "가족의 최신 quota 스냅샷을 찾을 수 없습니다."),
    FAMILY_QUOTA_UPDATE_FAILED(
            HttpStatus.CONFLICT, "FAMILY_004", "가족 quota를 현재 월 기준으로 갱신하지 못했습니다.");

    private final HttpStatus httpStatus;
    private final String customCode;
    private final String message;
}
