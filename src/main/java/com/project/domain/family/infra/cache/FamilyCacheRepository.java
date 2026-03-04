package com.project.domain.family.infra.cache;

import java.time.LocalDate;
import java.util.Optional;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import com.project.domain.family.entity.Family;
import com.project.domain.family.infra.cache.dto.FamilyCacheDto;
import com.project.global.common.TimeConstants;
import com.project.global.util.RedisKeyGenerator;

import lombok.RequiredArgsConstructor;

@Repository
@RequiredArgsConstructor
public class FamilyCacheRepository {

    private final RedisTemplate<String, String> familyStringRedisTemplate;
    private final RedisTemplate<String, FamilyCacheDto> familyCacheRedisTemplate;
    private final RedisKeyGenerator redisKeyGenerator;

    public void save(Family family) {
        String key = redisKeyGenerator.generateFamilyInfoKey(family.getId());
        familyCacheRedisTemplate.opsForValue().set(key, FamilyCacheDto.from(family));
    }

    public Optional<Family> findById(Long familyId) {
        String key = redisKeyGenerator.generateFamilyInfoKey(familyId);
        FamilyCacheDto dto = familyCacheRedisTemplate.opsForValue().get(key);

        if (dto == null) {
            return Optional.empty();
        }

        return Optional.of(dto.toEntity());
    }

    public void evict(Long familyId) {
        String key = redisKeyGenerator.generateFamilyInfoKey(familyId);
        familyCacheRedisTemplate.delete(key);
    }

    public Optional<Long> findFamilyRemainingBytes(Long familyId) {
        String key = redisKeyGenerator.generateFamilyRemainingKey(familyId);
        return findLongValue(key);
    }

    public Optional<Long> findCustomerMonthlyUsageBytes(Long familyId, Long customerId) {
        LocalDate currentMonth = LocalDate.now(TimeConstants.ASIA_SEOUL).withDayOfMonth(1);
        String key =
                redisKeyGenerator.generateFamilyCustomerMonthlyUsageKey(
                        familyId, customerId, currentMonth);
        return findLongValue(key);
    }

    private Optional<Long> findLongValue(String key) {
        String raw = familyStringRedisTemplate.opsForValue().get(key);

        if (raw == null || raw.isBlank()) {
            return Optional.empty();
        }

        try {
            return Optional.of(Long.parseLong(raw));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }
}
