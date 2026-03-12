package com.project.global.util;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.springframework.stereotype.Component;

@Component
public class RedisKeyGenerator {

    private static final String KEY_SEPARATOR = ":";
    private static final String FAMILY_KEY_PREFIX = "family";
    private static final String POLICY_EVENT_DEDUP_KEY_PREFIX = "event:dedup:policy";
    private static final String USAGE_EVENT_DEDUP_KEY_PREFIX = "event:dedup:usage";
    private static final DateTimeFormatter MONTH_SUFFIX_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMM");

    // 가족 알림 상태 키
    public String generateFamilyAlertsKey(Long familyId) {
        return FAMILY_KEY_PREFIX
                + KEY_SEPARATOR
                + familyId
                + KEY_SEPARATOR
                + "alert"
                + KEY_SEPARATOR
                + "THRESHOLD";
    }

    // 가족 quota 정보 키
    public String generateFamilyInfoKey(Long familyId) {
        return FAMILY_KEY_PREFIX + KEY_SEPARATOR + familyId + KEY_SEPARATOR + "info";
    }

    // 가족 잔여 데이터 키
    public String generateFamilyRemainingKey(Long familyId) {
        return FAMILY_KEY_PREFIX + KEY_SEPARATOR + familyId + KEY_SEPARATOR + "remaining";
    }

    // 고객 월별 사용량 키
    public String generateFamilyCustomerMonthlyUsageKey(
            Long familyId, Long customerId, LocalDate eventMonth) {
        return FAMILY_KEY_PREFIX
                + KEY_SEPARATOR
                + familyId
                + KEY_SEPARATOR
                + "customer"
                + KEY_SEPARATOR
                + customerId
                + KEY_SEPARATOR
                + "usage"
                + KEY_SEPARATOR
                + "monthly"
                + KEY_SEPARATOR
                + eventMonth.format(MONTH_SUFFIX_FORMATTER);
    }

    // 고객 정책 제약 키
    public String generateFamilyCustomerConstraintsKey(Long familyId, Long customerId) {
        return FAMILY_KEY_PREFIX
                + KEY_SEPARATOR
                + familyId
                + KEY_SEPARATOR
                + "customer"
                + KEY_SEPARATOR
                + customerId
                + KEY_SEPARATOR
                + "constraints";
    }

    // policy 이벤트 dedup 키
    public String generatePolicyEventDedupKey(String eventId, Long customerId) {
        return POLICY_EVENT_DEDUP_KEY_PREFIX + KEY_SEPARATOR + eventId + KEY_SEPARATOR + customerId;
    }

    // usage-event dedup 키
    public String generateUsageEventDedupKey(String eventId) {
        return USAGE_EVENT_DEDUP_KEY_PREFIX + KEY_SEPARATOR + eventId;
    }
}
