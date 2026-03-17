package com.project.domain.usage.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.LocalDate;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.dabom.messaging.kafka.event.dto.usage.UsagePayload;
import com.project.domain.family.repository.FamilyMemberRepository;
import com.project.domain.usage.service.dto.UsagePersistPayload;
import com.project.domain.usage.service.helper.CustomerQuotaWriter;
import com.project.domain.usage.service.helper.FamilyQuotaWriter;
import com.project.domain.usage.service.helper.UsagePersistEventValidator;
import com.project.domain.usage.service.helper.UsageRecordWriter;
import com.project.global.util.LogSanitizer;

@ExtendWith(MockitoExtension.class)
class UsagePersistServiceImplTest {

    @InjectMocks private UsagePersistServiceImpl usagePersistService;

    @Mock private FamilyMemberRepository familyMemberRepository;
    @Mock private UsagePersistEventValidator usagePersistEventValidator;
    @Mock private UsageRecordWriter usageRecordWriter;
    @Mock private CustomerQuotaWriter customerQuotaWriter;
    @Mock private FamilyQuotaWriter familyQuotaWriter;
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
    @DisplayName("ALLOWED 상태면 customer_quota와 family_quota를 함께 반영한다")
    void persistFromUsageEvent_AllowedStatus_PersistsAsAllowed() {
        String eventId = "evt_1";
        String eventTime = "2026-03-15T01:02:03";
        UsagePayload usagePayload = new UsagePayload(100L, 1L, "app", 2048L, Map.of());
        LocalDate eventMonth = LocalDate.of(2026, 3, 1);

        org.mockito.BDDMockito.given(
                        usagePersistEventValidator.isValidPayload(
                                any(UsagePersistPayload.class), any(), any()))
                .willReturn(true);
        org.mockito.BDDMockito.given(
                        familyMemberRepository.existsByFamilyIdAndCustomerIdAndDeletedAtIsNull(
                                100L, 1L))
                .willReturn(true);
        org.mockito.BDDMockito.given(usageRecordWriter.persistUsageRecord(any(), any(), any()))
                .willReturn(true);

        usagePersistService.persistFromUsageEvent(eventId, eventTime, usagePayload, "ALLOWED");

        verify(customerQuotaWriter, times(1))
                .persistAllowedQuota(
                        any(UsagePersistPayload.class), any(LocalDate.class), any(), any());
        verify(familyQuotaWriter, times(1))
                .persistAllowedQuota(100L, eventMonth, 2048L, eventId, eventId);
        verify(customerQuotaWriter, never()).persistBlockedQuota(any(), any(), any(), any(), any());
    }

    @Test
    @DisplayName("APP_BLOCK 상태면 usage_record 없이 차단 상태만 반영한다")
    void persistFromUsageEvent_AppBlock_OnlyPersistsBlockState() {
        String eventId = "evt_2";
        String eventTime = "2026-03-20T10:20:30";
        UsagePayload usagePayload = new UsagePayload(200L, 2L, "app", 1024L, Map.of());
        LocalDate eventMonth = LocalDate.of(2026, 3, 1);

        org.mockito.BDDMockito.given(
                        usagePersistEventValidator.isValidPayload(
                                any(UsagePersistPayload.class), any(), any()))
                .willReturn(true);
        org.mockito.BDDMockito.given(
                        familyMemberRepository.existsByFamilyIdAndCustomerIdAndDeletedAtIsNull(
                                200L, 2L))
                .willReturn(true);

        usagePersistService.persistFromUsageEvent(eventId, eventTime, usagePayload, "APP_BLOCK");

        verify(customerQuotaWriter, times(1))
                .persistBlockedQuota(
                        any(UsagePersistPayload.class),
                        org.mockito.ArgumentMatchers.eq(eventMonth),
                        org.mockito.ArgumentMatchers.eq(eventId),
                        org.mockito.ArgumentMatchers.eq(eventId),
                        org.mockito.ArgumentMatchers.eq("APP_BLOCK"));
        verify(usageRecordWriter, never()).persistUsageRecord(any(), any(), any());
        verify(customerQuotaWriter, never()).persistAllowedQuota(any(), any(), any(), any());
        verify(familyQuotaWriter, never()).persistAllowedQuota(any(), any(), any(), any(), any());
    }
}
