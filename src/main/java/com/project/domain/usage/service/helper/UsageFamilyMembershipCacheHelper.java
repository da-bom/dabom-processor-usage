package com.project.domain.usage.service.helper;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import com.dabom.messaging.kafka.error.KafkaMessageProcessingException;
import com.project.domain.family.repository.FamilyMemberRepository;
import com.project.global.util.RedisKeyGenerator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class UsageFamilyMembershipCacheHelper {

    private final StringRedisTemplate stringRedisTemplate;
    private final FamilyMemberRepository familyMemberRepository;
    private final RedisKeyGenerator redisKeyGenerator;

    @Value("${app.redis.membership-cache-ttl-seconds:600}")
    private long membershipCacheTtlSeconds;

    // family-customer 관계를 Redis 우선으로 검증하고 miss면 DB fallback 한다.
    public boolean isValidFamilyCustomer(long familyId, long customerId) {
        String membersKey = redisKeyGenerator.generateFamilyMembersKey(familyId);

        try {
            Boolean hasMembersKey = stringRedisTemplate.hasKey(membersKey);
            if (Boolean.TRUE.equals(hasMembersKey)) {
                Boolean isMember =
                        stringRedisTemplate
                                .opsForSet()
                                .isMember(membersKey, String.valueOf(customerId));
                if (Boolean.TRUE.equals(isMember)) {
                    return true;
                }

                // 캐시가 오래되었을 수 있으므로 false일 때도 DB fallback으로 한 번 더 검증한다.
                return fallbackAndCacheMembership(familyId, customerId, membersKey);
            }

            return warmupFamilyMembersAndCheck(familyId, customerId, membersKey);
        } catch (DataAccessException e) {
            log.error(
                    "Family membership cache access failed. familyId={}, customerId={}",
                    familyId,
                    customerId,
                    e);
            return checkMembershipFromDatabase(familyId, customerId, membersKey);
        }
    }

    // DB에서 가족 구성원 전체를 읽어 Redis set을 채운 뒤 포함 여부를 확인한다.
    private boolean warmupFamilyMembersAndCheck(long familyId, long customerId, String membersKey) {
        List<FamilyMemberRepository.FamilyMemberTargetProjection> members =
                familyMemberRepository.findAllActiveTargetsByFamilyId(familyId);
        if (members.isEmpty()) {
            return false;
        }

        String[] memberIds =
                members.stream()
                        .map(FamilyMemberRepository.FamilyMemberTargetProjection::getCustomerId)
                        .map(String::valueOf)
                        .toArray(String[]::new);

        stringRedisTemplate.opsForSet().add(membersKey, memberIds);
        stringRedisTemplate.expire(membersKey, membershipCacheTtlSeconds, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(
                stringRedisTemplate.opsForSet().isMember(membersKey, String.valueOf(customerId)));
    }

    // 캐시 불일치 시 DB fallback 결과를 Redis에 반영한다.
    private boolean fallbackAndCacheMembership(long familyId, long customerId, String membersKey) {
        boolean exists = checkMembershipFromDatabase(familyId, customerId, membersKey);
        if (exists) {
            stringRedisTemplate.opsForSet().add(membersKey, String.valueOf(customerId));
            stringRedisTemplate.expire(membersKey, membershipCacheTtlSeconds, TimeUnit.SECONDS);
        }
        return exists;
    }

    // DB fallback도 실패하면 retryable 예외로 전파해 membership 검증 자체를 재시도한다.
    private boolean checkMembershipFromDatabase(long familyId, long customerId, String membersKey) {
        try {
            return familyMemberRepository.existsByFamilyIdAndCustomerIdAndDeletedAtIsNull(
                    familyId, customerId);
        } catch (DataAccessException e) {
            throw new KafkaMessageProcessingException(
                    "Family membership lookup failed. familyId=%d customerId=%d key=%s"
                            .formatted(familyId, customerId, membersKey),
                    e);
        }
    }
}
