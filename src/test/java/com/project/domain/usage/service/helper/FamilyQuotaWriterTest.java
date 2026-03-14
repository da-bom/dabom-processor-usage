package com.project.domain.usage.service.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.LocalDate;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import com.project.domain.family.entity.FamilyQuota;
import com.project.domain.family.repository.FamilyQuotaRepository;
import com.project.global.util.LogSanitizer;

@ExtendWith(MockitoExtension.class)
class FamilyQuotaWriterTest {

    @InjectMocks private FamilyQuotaWriter familyQuotaWriter;

    @Mock private FamilyQuotaRepository familyQuotaRepository;
    @Mock private LogSanitizer logSanitizer;

    @BeforeEach
    void setUp() {
        lenient()
                .when(logSanitizer.sanitize(nullable(String.class)))
                .thenAnswer(
                        invocation -> {
                            String raw = invocation.getArgument(0);
                            return raw == null ? "null" : raw;
                        });
    }

    @Test
    @DisplayName("현재 월 row가 있으면 usedBytes를 누적 갱신한다")
    void persistAllowedQuota_UpdateExistingRow() {
        LocalDate eventMonth = LocalDate.of(2026, 3, 1);
        given(familyQuotaRepository.incrementUsedBytes(100L, eventMonth, 1024L)).willReturn(1);

        familyQuotaWriter.persistAllowedQuota(100L, eventMonth, 1024L, "evt_1", "origin_1");

        verify(familyQuotaRepository, times(1)).incrementUsedBytes(100L, eventMonth, 1024L);
        verify(familyQuotaRepository, never()).saveAndFlush(any());
    }

    @Test
    @DisplayName("현재 월 row가 없으면 최신 스냅샷 totalQuota를 이어받아 새 row를 만든다")
    void persistAllowedQuota_CreateCurrentMonthRow() {
        LocalDate eventMonth = LocalDate.of(2026, 3, 1);
        FamilyQuota latest =
                FamilyQuota.builder()
                        .id(1L)
                        .familyId(100L)
                        .currentMonth(LocalDate.of(2026, 2, 1))
                        .totalQuotaBytes(10000L)
                        .usedBytes(7000L)
                        .build();

        given(familyQuotaRepository.incrementUsedBytes(100L, eventMonth, 1024L)).willReturn(0);
        given(familyQuotaRepository.findLatestByFamilyIdForUpdate(100L))
                .willReturn(Optional.of(latest));

        familyQuotaWriter.persistAllowedQuota(100L, eventMonth, 1024L, "evt_1", "origin_1");

        ArgumentCaptor<FamilyQuota> quotaCaptor = ArgumentCaptor.forClass(FamilyQuota.class);
        verify(familyQuotaRepository).saveAndFlush(quotaCaptor.capture());
        assertEquals(100L, quotaCaptor.getValue().getFamilyId());
        assertEquals(eventMonth, quotaCaptor.getValue().getCurrentMonth());
        assertEquals(10000L, quotaCaptor.getValue().getTotalQuotaBytes());
        assertEquals(1024L, quotaCaptor.getValue().getUsedBytes());
    }

    @Test
    @DisplayName("insert race가 나면 update 재시도로 복구한다")
    void persistAllowedQuota_RecoverFromInsertRace() {
        LocalDate eventMonth = LocalDate.of(2026, 3, 1);
        FamilyQuota latest =
                FamilyQuota.builder()
                        .id(1L)
                        .familyId(100L)
                        .currentMonth(LocalDate.of(2026, 2, 1))
                        .totalQuotaBytes(10000L)
                        .usedBytes(7000L)
                        .build();

        given(familyQuotaRepository.incrementUsedBytes(100L, eventMonth, 1024L))
                .willReturn(0)
                .willReturn(1);
        given(familyQuotaRepository.findLatestByFamilyIdForUpdate(100L))
                .willReturn(Optional.of(latest));
        given(familyQuotaRepository.saveAndFlush(any(FamilyQuota.class)))
                .willThrow(new DataIntegrityViolationException("race"));

        familyQuotaWriter.persistAllowedQuota(100L, eventMonth, 1024L, "evt_1", "origin_1");

        verify(familyQuotaRepository, times(2)).incrementUsedBytes(100L, eventMonth, 1024L);
    }

    @Test
    @DisplayName("최신 스냅샷이 없으면 예외를 던진다")
    void persistAllowedQuota_ThrowsWhenLatestSnapshotMissing() {
        LocalDate eventMonth = LocalDate.of(2026, 3, 1);
        given(familyQuotaRepository.incrementUsedBytes(100L, eventMonth, 1024L)).willReturn(0);
        given(familyQuotaRepository.findLatestByFamilyIdForUpdate(100L))
                .willReturn(Optional.empty());

        assertThrows(
                IllegalStateException.class,
                () ->
                        familyQuotaWriter.persistAllowedQuota(
                                100L, eventMonth, 1024L, "evt_1", "origin_1"));
    }
}
