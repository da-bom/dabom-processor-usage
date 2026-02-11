package com.project.global.exception;

public record ErrorResponse(int status, String code, String message) {}
