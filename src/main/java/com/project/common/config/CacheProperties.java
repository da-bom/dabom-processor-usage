package com.project.common.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.cache")
public class CacheProperties {

    private Duration defaultTtl = Duration.ofMinutes(5);
    private Map<String, Duration> ttl = new HashMap<>();

    public Duration getDefaultTtl() {
        return defaultTtl;
    }

    public void setDefaultTtl(Duration defaultTtl) {
        this.defaultTtl = defaultTtl;
    }

    public Map<String, Duration> getTtl() {
        return ttl;
    }

    public void setTtl(Map<String, Duration> ttl) {
        this.ttl = ttl;
    }
}
