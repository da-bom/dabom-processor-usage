package com.project.domain.usage.service;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import com.project.global.util.LogSanitizer;
import com.project.global.util.RedisKeyGenerator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class UsagePersistDedupService {
    private static final String DEDUP_FLAG = "1";

    private final RedisTemplate<String, String> familyStringRedisTemplate;
    private final RedisKeyGenerator redisKeyGenerator;
    private final LogSanitizer logSanitizer;

    @Value("${app.kafka.dedup.usage-persist-ttl-seconds}")
    private long usagePersistDedupTtlSeconds;

    // true면 이미 처리된 이벤트로 간주하고 이후 DB 반영을 생략한다.
    public boolean isDuplicated(String originEventId) {
        String dedupKey = redisKeyGenerator.generateUsagePersistEventDedupKey(originEventId);
        try {
            // 짧은 윈도우 재전송은 Redis에서 먼저 차단한다.
            Boolean firstSeen =
                    familyStringRedisTemplate
                            .opsForValue()
                            .setIfAbsent(
                                    dedupKey,
                                    DEDUP_FLAG,
                                    Duration.ofSeconds(usagePersistDedupTtlSeconds));
            if (!Boolean.TRUE.equals(firstSeen)) {
                log.info(
                        "Skip duplicated usage-persist event. originEventId={}",
                        logSanitizer.sanitize(originEventId));
                return true;
            }
            return false;
        } catch (DataAccessException e) {
            // Redis 장애 시에도 DB 반영을 우선한다.
            log.warn(
                    "Redis dedup failed. Continue DB persist for availability. originEventId={}",
                    logSanitizer.sanitize(originEventId),
                    e);
            return false;
        }
    }
}
