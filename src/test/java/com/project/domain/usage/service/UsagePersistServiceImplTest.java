package com.project.domain.usage.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.LocalDate;
import java.time.LocalDateTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.project.domain.family.repository.FamilyMemberRepository;
import com.project.domain.usage.service.helper.CustomerQuotaWriter;
import com.project.domain.usage.service.helper.FamilyUsageWriter;
import com.project.domain.usage.service.helper.UsagePersistEventValidator;
import com.project.domain.usage.service.helper.UsageRecordWriter;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.usage.UsagePersistPayload;
import com.project.global.util.LogSanitizer;

@ExtendWith(MockitoExtension.class)
class UsagePersistServiceImplTest {

    @InjectMocks private UsagePersistServiceImpl usagePersistService;

    @Mock private FamilyMemberRepository familyMemberRepository;
    @Mock private UsagePersistEventValidator usagePersistEventValidator;
    @Mock private UsageRecordWriter usageRecordWriter;
    @Mock private CustomerQuotaWriter customerQuotaWriter;
    @Mock private FamilyUsageWriter familyUsageWriter;
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
    @DisplayName("허용 이벤트면 eventMonth 기준으로 quota와 family를 함께 갱신한다")
    void persist_AllowedEvent_UpdatesQuotaAndFamilyByEventMonth() {
        UsagePersistPayload payload =
                new UsagePersistPayload(
                        "origin_1", 100L, 1L, 2048L, "app", "ALLOWED", "2026-03-15T01:02:03");
        EventEnvelope<UsagePersistPayload> envelope =
                new EventEnvelope<>("evt_1", "USAGE_PERSIST", null, LocalDateTime.now(), payload);
        LocalDate eventMonth = LocalDate.of(2026, 3, 1);

        given(usagePersistEventValidator.isValidPayload(payload, "evt_1", "recordKey"))
                .willReturn(true);
        given(familyMemberRepository.existsByFamilyIdAndCustomerIdAndDeletedAtIsNull(100L, 1L))
                .willReturn(true);
        given(usageRecordWriter.persistUsageRecord(payload, "evt_1", "origin_1")).willReturn(true);

        usagePersistService.persist(envelope, "recordKey");

        verify(customerQuotaWriter, times(1))
                .persistAllowedQuota(payload, eventMonth, "evt_1", "origin_1");
        verify(familyUsageWriter, times(1))
                .updateFamilyUsedBytes(100L, eventMonth, 2048L, "evt_1", "origin_1");
        verify(customerQuotaWriter, never()).persistBlockedQuota(any(), any(), any(), any(), any());
    }

    @Test
    @DisplayName("차단 이벤트면 usage_record와 family 누적 없이 차단 상태만 반영한다")
    void persist_BlockedEvent_OnlyPersistsBlockState() {
        UsagePersistPayload payload =
                new UsagePersistPayload(
                        "origin_2", 200L, 2L, 1024L, "app", "TIME_BLOCK", "2026-03-20T10:20:30");
        EventEnvelope<UsagePersistPayload> envelope =
                new EventEnvelope<>("evt_2", "USAGE_PERSIST", null, LocalDateTime.now(), payload);
        LocalDate eventMonth = LocalDate.of(2026, 3, 1);

        given(usagePersistEventValidator.isValidPayload(payload, "evt_2", "recordKey"))
                .willReturn(true);
        given(familyMemberRepository.existsByFamilyIdAndCustomerIdAndDeletedAtIsNull(200L, 2L))
                .willReturn(true);

        usagePersistService.persist(envelope, "recordKey");

        verify(customerQuotaWriter, times(1))
                .persistBlockedQuota(payload, eventMonth, "evt_2", "origin_2", "TIME_BLOCK");
        verify(usageRecordWriter, never()).persistUsageRecord(any(), any(), any());
        verify(customerQuotaWriter, never()).persistAllowedQuota(any(), any(), any(), any());
        verify(familyUsageWriter, never()).updateFamilyUsedBytes(any(), any(), any(), any(), any());
    }
}
