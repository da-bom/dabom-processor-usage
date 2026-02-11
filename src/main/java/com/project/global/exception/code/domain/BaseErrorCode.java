package com.project.global.exception.code.domain;

import org.springframework.http.HttpStatus;

public interface BaseErrorCode {
    HttpStatus getHttpStatus();

    String getCustomCode();

    String getMessage();

    String name();
}
