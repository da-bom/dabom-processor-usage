package com.project.domain.usage.service.helper;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import com.project.domain.customer.repository.CustomerQuotaRepository;
import com.project.domain.family.entity.Family;
import com.project.domain.family.entity.FamilyQuota;
import com.project.domain.family.repository.FamilyQuotaRepository;
import com.project.domain.family.repository.FamilyRepository;

@ExtendWith(MockitoExtension.class)
class UsageRedisWarmupHelperTest {

    @InjectMocks private UsageRedisWarmupHelper usageRedisWarmupHelper;

    @Mock private StringRedisTemplate stringRedisTemplate;
    @Mock private FamilyRepository familyRepository;
    @Mock private FamilyQuotaRepository familyQuotaRepository;
    @Mock private CustomerQuotaRepository customerQuotaRepository;
    @Mock private HashOperations<String, Object, Object> hashOperations;
    @Mock private ValueOperations<String, String> valueOperations;

    @BeforeEach
    void setUp() {
        lenient().when(stringRedisTemplate.opsForHash()).thenReturn(hashOperations);
        lenient().when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    @Test
    @DisplayName("현재 월 family_quota가 있으면 해당 totalQuota로 info hash를 채운다")
    void ensureFamilyInfoCached_UsesCurrentMonthQuota() {
        long familyId = 100L;
        LocalDate eventMonth = LocalDate.of(2026, 3, 1);
        String key = "family:100:info:202603";
        Family family = Family.builder().id(familyId).name("우리집").createdById(1L).build();
        ReflectionTestUtils.setField(
                family, "createdAt", LocalDateTime.parse("2026-01-01T10:00:00"));
        FamilyQuota familyQuota =
                FamilyQuota.builder()
                        .familyId(familyId)
                        .currentMonth(eventMonth)
                        .totalQuotaBytes(10000L)
                        .usedBytes(3000L)
                        .build();

        given(hashOperations.entries(key)).willReturn(Map.of());
        given(familyRepository.findById(familyId)).willReturn(Optional.of(family));
        given(familyQuotaRepository.findActiveByFamilyIdAndCurrentMonth(familyId, eventMonth))
                .willReturn(Optional.of(familyQuota));

        boolean result = usageRedisWarmupHelper.ensureFamilyInfoCached(familyId, eventMonth, key);

        assertTrue(result);
        verify(hashOperations).putAll(eq(key), anyMap());
    }

    @Test
    @DisplayName("현재 월 family_quota가 있으면 remaining은 total-used로 시드한다")
    void ensureRemainingBytesCached_UsesCurrentMonthQuota() {
        long familyId = 100L;
        LocalDate eventMonth = LocalDate.of(2026, 3, 1);
        String key = "family:100:remaining:202603";
        FamilyQuota familyQuota =
                FamilyQuota.builder()
                        .familyId(familyId)
                        .currentMonth(eventMonth)
                        .totalQuotaBytes(10000L)
                        .usedBytes(3000L)
                        .build();

        given(valueOperations.get(key)).willReturn(null);
        given(familyQuotaRepository.findActiveByFamilyIdAndCurrentMonth(familyId, eventMonth))
                .willReturn(Optional.of(familyQuota));
        given(valueOperations.setIfAbsent(key, "7000")).willReturn(true);

        boolean result =
                usageRedisWarmupHelper.ensureRemainingBytesCached(familyId, eventMonth, key);

        assertTrue(result);
        verify(valueOperations).setIfAbsent(key, "7000");
    }

    @Test
    @DisplayName("현재 월 row가 없고 최신 스냅샷만 있으면 remaining은 totalQuota 전체로 시드한다")
    void ensureRemainingBytesCached_SeedsFullQuotaWhenCurrentMonthMissing() {
        long familyId = 100L;
        LocalDate eventMonth = LocalDate.of(2026, 3, 1);
        String key = "family:100:remaining:202603";
        FamilyQuota latestSnapshot =
                FamilyQuota.builder()
                        .familyId(familyId)
                        .currentMonth(LocalDate.of(2026, 2, 1))
                        .totalQuotaBytes(9000L)
                        .usedBytes(8500L)
                        .build();

        given(valueOperations.get(key)).willReturn(null);
        given(familyQuotaRepository.findActiveByFamilyIdAndCurrentMonth(familyId, eventMonth))
                .willReturn(Optional.empty());
        given(
                        familyQuotaRepository
                                .findTopByFamilyIdAndDeletedAtIsNullOrderByCurrentMonthDesc(
                                        familyId))
                .willReturn(Optional.of(latestSnapshot));
        given(valueOperations.setIfAbsent(key, "9000")).willReturn(true);

        boolean result =
                usageRedisWarmupHelper.ensureRemainingBytesCached(familyId, eventMonth, key);

        assertTrue(result);
        verify(valueOperations).setIfAbsent(key, "9000");
    }

    @Test
    @DisplayName("최신 스냅샷도 없으면 remaining warmup은 실패한다")
    void ensureRemainingBytesCached_FailsWhenNoSnapshot() {
        long familyId = 100L;
        LocalDate eventMonth = LocalDate.of(2026, 3, 1);
        String key = "family:100:remaining:202603";

        given(valueOperations.get(key)).willReturn(null);
        given(familyQuotaRepository.findActiveByFamilyIdAndCurrentMonth(familyId, eventMonth))
                .willReturn(Optional.empty());
        given(
                        familyQuotaRepository
                                .findTopByFamilyIdAndDeletedAtIsNullOrderByCurrentMonthDesc(
                                        familyId))
                .willReturn(Optional.empty());

        boolean result =
                usageRedisWarmupHelper.ensureRemainingBytesCached(familyId, eventMonth, key);

        assertFalse(result);
    }
}
