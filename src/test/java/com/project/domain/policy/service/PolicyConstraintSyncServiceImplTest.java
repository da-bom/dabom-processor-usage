package com.project.domain.policy.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.LocalDateTime;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.util.ReflectionTestUtils;

import com.project.domain.family.repository.FamilyMemberRepository;
import com.project.domain.policy.constant.PolicyConstraintKeyConstants;
import com.project.domain.policy.service.helper.PolicyConstraintEventMapper;
import com.project.domain.policy.service.helper.PolicyEventValidator;
import com.project.global.event.dto.EventEnvelope;
import com.project.global.event.dto.policy.PolicyUpdatedPayload;
import com.project.global.util.LogSanitizer;
import com.project.global.util.RedisKeyGenerator;

@ExtendWith(MockitoExtension.class)
class PolicyConstraintSyncServiceImplTest {

    @InjectMocks private PolicyConstraintSyncServiceImpl service;

    @Mock private RedisTemplate<String, String> familyStringRedisTemplate;
    @Mock private RedisScript<List<String>> policyConstraintUpdateScript;
    @Mock private RedisKeyGenerator redisKeyGenerator;
    @Mock private FamilyMemberRepository familyMemberRepository;
    @Mock private PolicyEventValidator policyEventValidator;
    @Mock private PolicyConstraintEventMapper policyConstraintEventMapper;
    @Mock private LogSanitizer logSanitizer;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(service, "dedupTtlSeconds", 60L);
        lenient()
                .when(logSanitizer.sanitize(nullable(String.class)))
                .thenAnswer(
                        invocation -> {
                            String raw = invocation.getArgument(0);
                            return raw == null ? "null" : raw;
                        });
    }

    @Test
    @DisplayName("constraints key가 없으면 일반 정책 업데이트를 스킵한다")
    void sync_SkipWhenConstraintsKeyMissing() {
        PolicyUpdatedPayload payload =
                new PolicyUpdatedPayload(
                        10L, 20L, PolicyConstraintKeyConstants.LIMIT_DATA_MONTHLY, "1024", true);
        EventEnvelope<PolicyUpdatedPayload> envelope =
                new EventEnvelope<>("evt-1", "POLICY_UPDATED", null, LocalDateTime.now(), payload);

        given(policyEventValidator.isValidPayload(payload, "evt-1", "record-1")).willReturn(true);
        given(
                        policyEventValidator.isAllowedPolicyKey(
                                PolicyConstraintKeyConstants.LIMIT_DATA_MONTHLY))
                .willReturn(true);
        given(
                        policyConstraintEventMapper.normalizeValue(
                                PolicyConstraintKeyConstants.LIMIT_DATA_MONTHLY, "1024"))
                .willReturn("1024");
        given(familyMemberRepository.existsByFamilyIdAndCustomerIdAndDeletedAtIsNull(10L, 20L))
                .willReturn(true);
        given(redisKeyGenerator.generateFamilyCustomerConstraintsKey(10L, 20L))
                .willReturn("family:10:customer:20:constraints");
        given(familyStringRedisTemplate.hasKey("family:10:customer:20:constraints"))
                .willReturn(false);

        service.sync(envelope, "record-1");

        verify(familyStringRedisTemplate, never())
                .execute(eq(policyConstraintUpdateScript), anyList(), any(), any(), any(), any());
    }

    @Test
    @DisplayName("constraints key가 있으면 일반 정책 업데이트를 Lua로 반영한다")
    void sync_ApplyWhenConstraintsKeyExists() {
        PolicyUpdatedPayload payload =
                new PolicyUpdatedPayload(
                        10L, 20L, PolicyConstraintKeyConstants.LIMIT_DATA_MONTHLY, "1024", true);
        EventEnvelope<PolicyUpdatedPayload> envelope =
                new EventEnvelope<>("evt-2", "POLICY_UPDATED", null, LocalDateTime.now(), payload);

        given(policyEventValidator.isValidPayload(payload, "evt-2", "record-2")).willReturn(true);
        given(
                        policyEventValidator.isAllowedPolicyKey(
                                PolicyConstraintKeyConstants.LIMIT_DATA_MONTHLY))
                .willReturn(true);
        given(
                        policyConstraintEventMapper.normalizeValue(
                                PolicyConstraintKeyConstants.LIMIT_DATA_MONTHLY, "1024"))
                .willReturn("1024");
        given(familyMemberRepository.existsByFamilyIdAndCustomerIdAndDeletedAtIsNull(10L, 20L))
                .willReturn(true);
        given(redisKeyGenerator.generateFamilyCustomerConstraintsKey(10L, 20L))
                .willReturn("family:10:customer:20:constraints");
        given(familyStringRedisTemplate.hasKey("family:10:customer:20:constraints"))
                .willReturn(true);
        given(redisKeyGenerator.generatePolicyEventDedupKey("evt-2", 20L))
                .willReturn("event:dedup:policy:evt-2:20");
        given(
                        familyStringRedisTemplate.execute(
                                eq(policyConstraintUpdateScript),
                                anyList(),
                                any(),
                                any(),
                                any(),
                                any()))
                .willReturn(List.of("APPLIED", "HSET"));

        service.sync(envelope, "record-2");

        verify(familyStringRedisTemplate, times(1))
                .execute(eq(policyConstraintUpdateScript), anyList(), any(), any(), any(), any());
    }

    @Test
    @DisplayName("BLOCK:APP에서 constraints key가 없으면 diff 동기화를 수행하지 않는다")
    void sync_BlockAppSkipWhenConstraintsKeyMissing() {
        PolicyUpdatedPayload payload =
                new PolicyUpdatedPayload(
                        10L,
                        20L,
                        PolicyConstraintKeyConstants.BLOCK_APP,
                        "[\"app1\",\"app2\"]",
                        true);
        EventEnvelope<PolicyUpdatedPayload> envelope =
                new EventEnvelope<>("evt-3", "POLICY_UPDATED", null, LocalDateTime.now(), payload);

        given(policyEventValidator.isValidPayload(payload, "evt-3", "record-3")).willReturn(true);
        given(policyEventValidator.isAllowedPolicyKey(PolicyConstraintKeyConstants.BLOCK_APP))
                .willReturn(true);
        given(policyConstraintEventMapper.normalizeAppBlockValueAsSet("[\"app1\",\"app2\"]"))
                .willReturn(new LinkedHashSet<>(Set.of("app1", "app2")));
        given(familyMemberRepository.existsByFamilyIdAndCustomerIdAndDeletedAtIsNull(10L, 20L))
                .willReturn(true);
        given(redisKeyGenerator.generateFamilyCustomerConstraintsKey(10L, 20L))
                .willReturn("family:10:customer:20:constraints");
        given(familyStringRedisTemplate.hasKey("family:10:customer:20:constraints"))
                .willReturn(false);

        service.sync(envelope, "record-3");

        verify(familyStringRedisTemplate, never()).opsForHash();
        verify(familyStringRedisTemplate, never())
                .execute(eq(policyConstraintUpdateScript), anyList(), any(), any(), any(), any());
    }
}
