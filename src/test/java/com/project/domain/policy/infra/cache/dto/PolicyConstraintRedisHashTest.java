package com.project.domain.policy.infra.cache.dto;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PolicyConstraintRedisHashTest {

    @Test
    @DisplayName("putMonthlyLimit은 LIMIT:DATA:MONTHLY 필드에 바이트 값을 저장한다")
    void putMonthlyLimit_storesLimitBytesUnderMonthlyLimitField() {
        PolicyConstraintRedisHash hash = PolicyConstraintRedisHash.create();

        hash.putMonthlyLimit(1073741824L);

        Map<String, String> result = hash.toMap();
        assertThat(result).containsEntry("LIMIT:DATA:MONTHLY", "1073741824");
    }

    @Test
    @DisplayName("putTimeBlockRange는 BLOCK:TIME 필드에 시간 범위를 저장한다")
    void putTimeBlockRange_storesTimeRangeUnderTimeBlockField() {
        PolicyConstraintRedisHash hash = PolicyConstraintRedisHash.create();

        hash.putTimeBlockRange("2200-0700");

        Map<String, String> result = hash.toMap();
        assertThat(result).containsEntry("BLOCK:TIME", "2200-0700");
    }

    @Test
    @DisplayName("putManualBlock은 BLOCK:ACCESS 필드에 1을 저장한다")
    void putManualBlock_storesOneUnderAccessBlockField() {
        PolicyConstraintRedisHash hash = PolicyConstraintRedisHash.create();

        hash.putManualBlock();

        Map<String, String> result = hash.toMap();
        assertThat(result).containsEntry("BLOCK:ACCESS", "1");
    }

    @Test
    @DisplayName("putBlockedApp은 BLOCK:APP:{appId} 필드에 1을 저장한다")
    void putBlockedApp_storesOneUnderAppBlockField() {
        PolicyConstraintRedisHash hash = PolicyConstraintRedisHash.create();

        hash.putBlockedApp("com.youtube.app");

        Map<String, String> result = hash.toMap();
        assertThat(result).containsEntry("BLOCK:APP:com.youtube.app", "1");
    }

    @Test
    @DisplayName("putBlockedApp은 appId의 앞뒤 공백을 제거하고 소문자로 저장한다")
    void putBlockedApp_trimsAndLowercasesAppId() {
        PolicyConstraintRedisHash hash = PolicyConstraintRedisHash.create();

        hash.putBlockedApp(" Com.YouTube.App ");

        Map<String, String> result = hash.toMap();
        assertThat(result).containsEntry("BLOCK:APP:com.youtube.app", "1");
    }

    @Test
    @DisplayName("여러 정책을 동시에 저장하면 toMap이 모든 필드를 반환한다")
    void toMap_returnsAllFieldsWhenMultiplePoliciesStored() {
        PolicyConstraintRedisHash hash = PolicyConstraintRedisHash.create();

        hash.putMonthlyLimit(500000000L);
        hash.putTimeBlockRange("2300-0600");
        hash.putManualBlock();
        hash.putBlockedApp("com.instagram.app");

        Map<String, String> result = hash.toMap();
        assertThat(result)
                .containsEntry("LIMIT:DATA:MONTHLY", "500000000")
                .containsEntry("BLOCK:TIME", "2300-0600")
                .containsEntry("BLOCK:ACCESS", "1")
                .containsEntry("BLOCK:APP:com.instagram.app", "1")
                .hasSize(4);
    }

    @Test
    @DisplayName("putBlockedApp을 여러 번 호출하면 각 appId가 독립 필드로 저장된다")
    void putBlockedApp_storesEachAppAsIndependentField() {
        PolicyConstraintRedisHash hash = PolicyConstraintRedisHash.create();

        hash.putBlockedApp("com.tiktok.app");
        hash.putBlockedApp("com.instagram.app");

        Map<String, String> result = hash.toMap();
        assertThat(result)
                .containsEntry("BLOCK:APP:com.tiktok.app", "1")
                .containsEntry("BLOCK:APP:com.instagram.app", "1")
                .hasSize(2);
    }

    @Test
    @DisplayName("create로 생성한 해시는 초기 상태가 비어 있다")
    void create_returnsEmptyMapInitially() {
        PolicyConstraintRedisHash hash = PolicyConstraintRedisHash.create();

        assertThat(hash.toMap()).isEmpty();
    }
}
