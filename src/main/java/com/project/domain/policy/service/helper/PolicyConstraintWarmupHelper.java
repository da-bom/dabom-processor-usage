package com.project.domain.policy.service.helper;

import java.util.Map;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import com.project.global.util.RedisKeyGenerator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class PolicyConstraintWarmupHelper {

    private final RedisTemplate<String, String> familyStringRedisTemplate;
    private final RedisKeyGenerator redisKeyGenerator;
    private final PolicyAssignmentSyncHelper policyAssignmentSyncHelper;

    // Lua 실행 전에 constraints 해시가 없을 때 DB 기준으로 1회 복구하는 메서드
    public void warmupIfMissing(Long familyId, Long customerId) {
        String constraintsKey =
                redisKeyGenerator.generateFamilyCustomerConstraintsKey(familyId, customerId);

        // 이미 Redis에 값이 있으면 warmup을 생략해 불필요한 DB 조회를 막는다.
        Boolean exists = familyStringRedisTemplate.hasKey(constraintsKey);
        if (Boolean.TRUE.equals(exists)) {
            return;
        }

        // DB assignment를 기준으로 effective constraints를 계산한다.
        Map<String, String> constraints =
                policyAssignmentSyncHelper.loadEffectiveConstraints(familyId, customerId);
        if (constraints.isEmpty()) {
            // DB에도 적용 정책이 없으면 Redis key를 만들지 않고 종료한다.
            log.info(
                    "Skip constraints warmup due to empty DB rules. familyId={}, customerId={}",
                    familyId,
                    customerId);
            return;
        }

        // hash putAll로 키-값을 한 번에 채워 Lua가 즉시 읽을 수 있게 만든다.
        familyStringRedisTemplate.opsForHash().putAll(constraintsKey, constraints);

        log.info(
                "Warmed up constraints from DB to Redis. familyId={}, customerId={}, size={}",
                familyId,
                customerId,
                constraints.size());
    }
}
