package com.project.common.exception;

public record ErrorResponse(int status, String code, String message) {}
