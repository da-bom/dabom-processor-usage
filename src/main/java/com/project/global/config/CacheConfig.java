package com.project.global.config;

import java.util.List;

import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.project.domain.policy.service.PolicyAssignmentSyncService;

@Configuration
public class CacheConfig {
    // 캐시 이름 중앙 관리:
    // 신규 서비스 캐시를 추가할 때 이 리스트에 이름만 넣으면 된다.
    private static final List<String> CACHE_NAMES =
            List.of(
                    PolicyAssignmentSyncService.POLICY_CONSTRAINT_CACHE
                    // 예시:
                    // "familyDetailCache",
                    // "usageRuleCache"
                    );

    @Bean
    public CacheManager cacheManager() {
        ConcurrentMapCacheManager cacheManager = new ConcurrentMapCacheManager();
        cacheManager.setCacheNames(CACHE_NAMES);
        return cacheManager;
    }
}
