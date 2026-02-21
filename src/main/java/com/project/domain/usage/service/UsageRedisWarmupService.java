package com.project.domain.usage.service;

import java.time.LocalDate;
import java.util.Map;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import com.project.domain.customer.entity.CustomerQuota;
import com.project.domain.customer.repository.CustomerQuotaRepository;
import com.project.domain.family.entity.Family;
import com.project.domain.family.repository.FamilyRepository;
import com.project.domain.usage.infra.cache.dto.FamilyInfoRedisHash;
import com.project.domain.usage.service.dto.FamilyInfo;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class UsageRedisWarmupService {

    private final StringRedisTemplate stringRedisTemplate;
    private final FamilyRepository familyRepository;
    private final CustomerQuotaRepository customerQuotaRepository;

    public boolean ensureFamilyInfoCached(long familyId, String key) {
        try {
            // Redis 조회
            Map<Object, Object> hash = stringRedisTemplate.opsForHash().entries(key);
            if (hash != null && !hash.isEmpty()) {
                return true; // 이미 캐시 존재
            }

            // DB 조회
            Family entity = familyRepository.findById(familyId).orElse(null);
            if (entity == null) {
                log.warn("Family not found in DB during Redis fallback. familyId={}", familyId);
                return false;
            }

            FamilyInfo info =
                    new FamilyInfo(
                            familyId,
                            entity.getName(),
                            entity.getTotalQuotaBytes(),
                            entity.getCreatedAt());

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

    public boolean ensureRemainingBytesCached(long familyId, String key) {
        try {
            // Redis에 값이 있으면 성공
            String cached = stringRedisTemplate.opsForValue().get(key);
            if (cached != null) {
                return true;
            }

            // Redis에 없으면 DB 조회
            Family family = familyRepository.findById(familyId).orElse(null);
            if (family == null) {
                log.warn("Family not found in DB during Redis fallback. familyId={}", familyId);
                return false;
            }

            long remaining = Math.max(0L, family.getTotalQuotaBytes() - family.getUsedBytes());

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

    public boolean ensureCustomerUsageCached(long familyId, long customerId, String key) {
        try {
            // Redis에 이미 존재하면 성공
            if (stringRedisTemplate.hasKey(key)) {
                return true;
            }

            // Family 조회 (currentMonth 확보용)
            Family family = familyRepository.findById(familyId).orElse(null);
            if (family == null) {
                log.warn("Family not found. familyId={}", familyId);
                return false;
            }

            LocalDate currentMonth = family.getCurrentMonth();

            // CustomerQuota 조회
            CustomerQuota quota =
                    customerQuotaRepository
                            .findActiveByFamilyIdAndCustomerIdAndCurrentMonth(
                                    familyId, customerId, currentMonth)
                            .orElse(null);

            if (quota == null) {
                log.warn(
                        "CustomerQuota not found. familyId={}, customerId={}, currentMonth={}",
                        familyId,
                        customerId,
                        currentMonth);
                return false;
            }

            long usedBytes = Math.max(0L, quota.getMonthlyUsedBytes());

            // Redis에 생성
            Boolean written =
                    stringRedisTemplate.opsForValue().setIfAbsent(key, String.valueOf(usedBytes));

            if (Boolean.TRUE.equals(written)) {
                return true;
            }

            // setIfAbsent가 false면 다시 GET해서 존재 확인
            // 다른 스레드/인스턴스가 먼저 세팅했어도 그건 성공으로 판단
            return stringRedisTemplate.hasKey(key);

        } catch (DataAccessException e) {
            log.error(
                    "Data access error during usage cache warm-up. familyId={}, customerId={}",
                    familyId,
                    customerId,
                    e);
            return false;
        } catch (RuntimeException e) {
            log.error(
                    "Unexpected error during usage cache warm-up. familyId={}, customerId={}",
                    familyId,
                    customerId,
                    e);
            return false;
        }
    }
}
