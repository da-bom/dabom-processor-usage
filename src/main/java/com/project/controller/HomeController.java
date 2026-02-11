package com.project.controller;

import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HomeController {

    @Value("${spring.application.name:backend}")
    private String applicationName;

    @Value("${app.version:1.0.0}")
    private String version;

    @GetMapping("/")
    public ResponseEntity<Map<String, String>> home() {
        return ResponseEntity.ok(
                Map.of(
                        "status",
                        "ok",
                        "message",
                        applicationName + " is running",
                        "version",
                        version,
                        "docs",
                        "/swagger-ui.html"));
    }
}
