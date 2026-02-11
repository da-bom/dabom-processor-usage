package com.project.example.infra.cache;

import java.time.Duration;
import java.util.Optional;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import com.project.example.core.Example;
import com.project.example.infra.cache.dto.ExampleCacheDto;

import lombok.RequiredArgsConstructor;

/** Redis Cache 저장소 (Adapter) - RedisTemplate을 사용하여 캐시 처리 - 캐시 조회/저장/삭제 기능 제공 */
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

        return Optional.of(dto.toDomain());
    }

    public void evict(Long exampleId) {
        String key = KEY_PREFIX + exampleId;
        redisTemplate.delete(key);
    }
}
