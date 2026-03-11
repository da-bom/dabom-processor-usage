package com.project.domain.policy.infra.cache.dto;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PolicyConstraintRedisHashTest {

    @Test
    @DisplayName("차단 앱 키는 소문자로 정규화해 저장한다")
    void putBlockedApp_NormalizesToLowercase() {
        PolicyConstraintRedisHash hash = PolicyConstraintRedisHash.create();

        hash.putBlockedApp(" Com.YouTube.App ", 123L);

        assertThat(hash.toMap())
                .containsEntry("BLOCK:APP:com.youtube.app", "1")
                .containsEntry("ver:BLOCK:APP:com.youtube.app", "123");
    }
}
