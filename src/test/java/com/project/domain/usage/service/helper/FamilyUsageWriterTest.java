package com.project.domain.usage.service.helper;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.LocalDate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.project.domain.family.repository.FamilyRepository;
import com.project.global.util.LogSanitizer;

@ExtendWith(MockitoExtension.class)
class FamilyUsageWriterTest {

    @InjectMocks private FamilyUsageWriter familyUsageWriter;

    @Mock private FamilyRepository familyRepository;
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
    @DisplayName("family 월경계 업데이트가 성공하면 예외 없이 종료한다")
    void updateFamilyUsedBytes_Success() {
        LocalDate eventMonth = LocalDate.of(2026, 3, 1);
        given(familyRepository.updateUsedBytesByEventMonth(100L, eventMonth, 1024L)).willReturn(1);

        familyUsageWriter.updateFamilyUsedBytes(100L, eventMonth, 1024L, "evt_1", "origin_1");

        verify(familyRepository, times(1)).updateUsedBytesByEventMonth(100L, eventMonth, 1024L);
    }

    @Test
    @DisplayName("family row 업데이트에 실패하면 예외를 던진다")
    void updateFamilyUsedBytes_Fail_ThrowsException() {
        LocalDate eventMonth = LocalDate.of(2026, 3, 1);
        given(familyRepository.updateUsedBytesByEventMonth(100L, eventMonth, 1024L)).willReturn(0);

        assertThrows(
                IllegalStateException.class,
                () ->
                        familyUsageWriter.updateFamilyUsedBytes(
                                100L, eventMonth, 1024L, "evt_1", "origin_1"));
    }
}
