package com.project.global.exception;

import com.project.global.exception.code.domain.BaseErrorCode;

public class ApplicationException extends BaseException {

    public ApplicationException(BaseErrorCode code) {
        super(code);
    }
}
