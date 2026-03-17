package com.project.global.config;

import java.time.Clock;
import java.time.ZoneId;

import org.springframework.context.annotation.Bean;

public class TimeConfig {
    public static final ZoneId ASIA_SEOUL = ZoneId.of("Asia/Seoul");

    @Bean
    public Clock clock() {
        return Clock.system(ASIA_SEOUL);
    }
}
