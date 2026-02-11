package com.project.global.exception.code;

import org.springframework.http.HttpStatus;

public interface BaseErrorCode {
    HttpStatus getHttpStatus();

    String getCustomCode();

    String getMessage();

    String name();
}
