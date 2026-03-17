package com.project.domain.usage.service.helper;

import java.time.LocalDate;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.stereotype.Service;

import com.project.common.config.TimeConfig;
import com.project.domain.customer.entity.CustomerQuota;
import com.project.domain.customer.repository.CustomerQuotaRepository;
import com.project.domain.family.entity.Family;
import com.project.domain.family.entity.FamilyQuota;
import com.project.domain.family.repository.FamilyQuotaRepository;
import com.project.domain.family.repository.FamilyRepository;
import com.project.domain.usage.infra.cache.dto.FamilyInfoRedisHash;
import com.project.domain.usage.service.dto.FamilyInfo;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class UsageRedisWarmupHelper {

    private final StringRedisTemplate stringRedisTemplate;
    private final FamilyRepository familyRepository;
    private final FamilyQuotaRepository familyQuotaRepository;
    private final CustomerQuotaRepository customerQuotaRepository;

    public boolean ensureFamilyInfoCached(long familyId, LocalDate eventMonth, String key) {
        try {
            // Redis 조회
            Map<Object, Object> hash = stringRedisTemplate.opsForHash().entries(key);
            if (hash != null && !hash.isEmpty()) {
                return true; // 이미 캐시 존재
            }

            // family와 family_quota 스냅샷을 조합해서 월별 info hash를 채움
            Family family = familyRepository.findById(familyId).orElse(null);
            if (family == null) {
                log.warn("Family not found in DB during Redis fallback. familyId={}", familyId);
                return false;
            }

            FamilyQuota familyQuota = resolveFamilyQuotaForInfo(familyId, eventMonth);
            if (familyQuota == null) {
                log.warn(
                        "Family quota snapshot not found in DB during info warmup. familyId={},"
                                + " eventMonth={}",
                        familyId,
                        eventMonth);
                return false;
            }

            FamilyInfo info =
                    new FamilyInfo(
                            familyId,
                            family.getName(),
                            familyQuota.getTotalQuotaBytes(),
                            family.getCreatedAt());

            // Redis 저장
            stringRedisTemplate.opsForHash().putAll(key, FamilyInfoRedisHash.toHash(info));
            return true;
        } catch (DataAccessException e) {
            log.error("Data access error during ensureFamilyInfoCached. familyId={}", familyId, e);
            return false;
        } catch (RuntimeException e) {
            log.error("Unexpected error during ensureFamilyInfoCached. familyId={}", familyId, e);
            return false;
        }
    }

    public boolean ensureRemainingBytesCached(long familyId, LocalDate eventMonth, String key) {
        try {
            // Redis에 값이 있으면 성공
            String cached = stringRedisTemplate.opsForValue().get(key);
            if (cached != null) {
                return true;
            }

            // 현재 월 row가 있으면 실제 잔여량을 쓰고 없으면 최신 총량으로 월초 상태를 시드함
            long remaining;
            FamilyQuota currentMonthQuota =
                    familyQuotaRepository
                            .findActiveByFamilyIdAndCurrentMonth(familyId, eventMonth)
                            .orElse(null);
            if (currentMonthQuota != null) {
                remaining =
                        Math.max(
                                0L,
                                currentMonthQuota.getTotalQuotaBytes()
                                        - currentMonthQuota.getUsedBytes());
            } else {
                FamilyQuota latestSnapshot =
                        familyQuotaRepository
                                .findTopByFamilyIdAndDeletedAtIsNullOrderByCurrentMonthDesc(
                                        familyId)
                                .orElse(null);
                if (latestSnapshot == null) {
                    log.warn(
                            "Family quota snapshot not found in DB during remaining warmup."
                                    + " familyId={}, eventMonth={}",
                            familyId,
                            eventMonth);
                    return false;
                }
                remaining = latestSnapshot.getTotalQuotaBytes();
            }

            // Redis에 쓰기
            Boolean written =
                    stringRedisTemplate.opsForValue().setIfAbsent(key, String.valueOf(remaining));
            if (Boolean.TRUE.equals(written)) {
                return true;
            }

            // setIfAbsent가 false면 다시 GET해서 존재 확인
            // 다른 스레드/인스턴스가 먼저 세팅했어도 그건 성공으로 판단
            return stringRedisTemplate.hasKey(key);
        } catch (DataAccessException e) {
            // Redis/DB 접근 계층 예외 (스프링 데이터 공통)
            log.error(
                    "Data access error during ensureRemainingBytesCached. familyId={}",
                    familyId,
                    e);
            return false;
        } catch (RuntimeException e) {
            // 그 외 예외도 false 처리
            log.error(
                    "Unexpected error during ensureRemainingBytesCached. familyId={}", familyId, e);
            return false;
        }
    }

    public boolean ensureCustomerUsageCached(
            long familyId, long customerId, String key, LocalDate eventMonth) {
        try {
            // Redis에 이미 존재하면 성공
            Boolean exists = stringRedisTemplate.hasKey(key);
            if (Boolean.TRUE.equals(exists)) {
                return true;
            }

            // 월 row가 없어도 첫 이벤트 처리를 위해 0으로 시드함
            CustomerQuota quota =
                    customerQuotaRepository
                            .findActiveByFamilyIdAndCustomerIdAndCurrentMonth(
                                    familyId, customerId, eventMonth)
                            .orElse(null);

            long usedBytes = (quota == null) ? 0L : Math.max(0L, quota.getMonthlyUsedBytes());
            long nextMonthStartEpochSecond =
                    eventMonth.plusMonths(1).atStartOfDay(TimeConfig.ASIA_SEOUL).toEpochSecond();

            // 키가 없어도 월초 첫 트래픽을 안전하게 처리하도록 0으로 시드하고 만료를 설정함
            Boolean written =
                    stringRedisTemplate.execute(
                            (RedisCallback<Boolean>)
                                    connection -> {
                                        StringRedisConnection redisConnection =
                                                (StringRedisConnection) connection;
                                        return redisConnection.set(
                                                key,
                                                String.valueOf(usedBytes),
                                                Expiration.unixTimestamp(
                                                        nextMonthStartEpochSecond,
                                                        TimeUnit.SECONDS),
                                                SetOption.SET_IF_ABSENT);
                                    });

            if (Boolean.TRUE.equals(written)) {
                return true;
            }

            // setIfAbsent가 false면 다시 GET해서 존재 확인
            // 다른 스레드/인스턴스가 먼저 세팅했어도 그건 성공으로 판단
            return stringRedisTemplate.hasKey(key);
        } catch (DataAccessException e) {
            log.error(
                    "Data access error during usage cache warm-up. familyId={}, customerId={},"
                            + " eventMonth={}",
                    familyId,
                    customerId,
                    eventMonth,
                    e);
            return false;
        } catch (RuntimeException e) {
            log.error(
                    "Unexpected error during usage cache warm-up. familyId={}, customerId={},"
                            + " eventMonth={}",
                    familyId,
                    customerId,
                    eventMonth,
                    e);
            return false;
        }
    }

    private FamilyQuota resolveFamilyQuotaForInfo(long familyId, LocalDate eventMonth) {
        // info hash는 현재 월 스냅샷을 우선하고 없으면 최신 스냅샷으로 대체함
        return familyQuotaRepository
                .findActiveByFamilyIdAndCurrentMonth(familyId, eventMonth)
                .or(
                        () ->
                                familyQuotaRepository
                                        .findTopByFamilyIdAndDeletedAtIsNullOrderByCurrentMonthDesc(
                                                familyId))
                .orElse(null);
    }
}
