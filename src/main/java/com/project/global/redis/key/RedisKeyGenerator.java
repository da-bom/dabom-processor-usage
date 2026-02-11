package com.project.global.redis.key;

import org.springframework.stereotype.Component;

@Component
public class RedisKeyGenerator {

    private static final String KEY_SEPARATOR = ":";
    private static final String EXAMPLE_KEY_PREFIX = "example";

    public String generateExampleKey(Long exampleId) {
        return EXAMPLE_KEY_PREFIX + KEY_SEPARATOR + exampleId;
    }
}
