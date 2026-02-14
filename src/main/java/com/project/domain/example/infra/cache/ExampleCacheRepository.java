package com.project.domain.example.infra.cache;

import java.time.Duration;
import java.util.Optional;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import com.project.domain.example.entity.Example;
import com.project.domain.example.infra.cache.dto.ExampleCacheDto;

import lombok.RequiredArgsConstructor;

@Repository
@RequiredArgsConstructor
public class ExampleCacheRepository {

    private final RedisTemplate<String, ExampleCacheDto> redisTemplate;
    private static final String KEY_PREFIX = "example:";
    private static final Duration TTL = Duration.ofMinutes(10);

    public void save(Example example) {
        String key = KEY_PREFIX + example.getExampleId();
        ExampleCacheDto dto = ExampleCacheDto.from(example);

        redisTemplate.opsForValue().set(key, dto);
    }

    public Optional<Example> findById(Long exampleId) {
        String key = KEY_PREFIX + exampleId;
        ExampleCacheDto dto = redisTemplate.opsForValue().get(key);

        if (dto == null) {
            return Optional.empty();
        }

        return Optional.of(dto.toEntity());
    }

    public void evict(Long exampleId) {
        String key = KEY_PREFIX + exampleId;
        redisTemplate.delete(key);
    }
}
