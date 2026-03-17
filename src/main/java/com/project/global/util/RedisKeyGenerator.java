package com.project.global.util;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.springframework.stereotype.Component;

@Component
public class RedisKeyGenerator {

    private static final String KEY_SEPARATOR = ":";
    private static final String ALERT_SEGMENT = "alert";
    private static final String FAMILY_KEY_PREFIX = "family";
    private static final String POLICY_EVENT_DEDUP_KEY_PREFIX = "event:dedup:policy";
    private static final String USAGE_EVENT_DEDUP_KEY_PREFIX = "event:dedup:usage";
    private static final DateTimeFormatter MONTH_SUFFIX_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMM");

    // 개인 기준 경고 알림 키를 만든다.
    public String generateFamilyCustomerThresholdAlertKey(
            Long familyId, Long customerId, int threshold, LocalDate eventMonth) {
        return familyCustomerPrefix(familyId, customerId)
                + KEY_SEPARATOR
                + ALERT_SEGMENT
                + KEY_SEPARATOR
                + "THRESHOLD"
                + KEY_SEPARATOR
                + threshold
                + KEY_SEPARATOR
                + formatMonth(eventMonth);
    }

    // 개인 기준 차단 알림 키를 만든다.
    public String generateFamilyCustomerBlockAlertKey(
            Long familyId, Long customerId, String alertType, LocalDate eventMonth) {
        return familyCustomerPrefix(familyId, customerId)
                + KEY_SEPARATOR
                + ALERT_SEGMENT
                + KEY_SEPARATOR
                + alertType
                + KEY_SEPARATOR
                + formatMonth(eventMonth);
    }

    // 앱 차단 알림은 앱 단위로 분리한다.
    public String generateFamilyCustomerAppBlockAlertKey(
            Long familyId, Long customerId, String appId, LocalDate eventMonth) {
        return familyCustomerPrefix(familyId, customerId)
                + KEY_SEPARATOR
                + ALERT_SEGMENT
                + KEY_SEPARATOR
                + "APP_BLOCK"
                + KEY_SEPARATOR
                + appId
                + KEY_SEPARATOR
                + formatMonth(eventMonth);
    }

    // 가족 구성원 set 키를 만든다.
    public String generateFamilyMembersKey(Long familyId) {
        return FAMILY_KEY_PREFIX + KEY_SEPARATOR + familyId + KEY_SEPARATOR + "members";
    }

    // 가족 quota 정보 키를 만든다.
    public String generateFamilyInfoKey(Long familyId, LocalDate eventMonth) {
        return FAMILY_KEY_PREFIX
                + KEY_SEPARATOR
                + familyId
                + KEY_SEPARATOR
                + "info"
                + KEY_SEPARATOR
                + formatMonth(eventMonth);
    }

    // 가족 남은 사용량 키를 만든다.
    public String generateFamilyRemainingKey(Long familyId, LocalDate eventMonth) {
        return FAMILY_KEY_PREFIX
                + KEY_SEPARATOR
                + familyId
                + KEY_SEPARATOR
                + "remaining"
                + KEY_SEPARATOR
                + formatMonth(eventMonth);
    }

    // 개인 월 사용량 키를 만든다.
    public String generateFamilyCustomerMonthlyUsageKey(
            Long familyId, Long customerId, LocalDate eventMonth) {
        return familyCustomerPrefix(familyId, customerId)
                + KEY_SEPARATOR
                + "usage"
                + KEY_SEPARATOR
                + "monthly"
                + KEY_SEPARATOR
                + formatMonth(eventMonth);
    }

    // 개인 제약 조건 키를 만든다.
    public String generateFamilyCustomerConstraintsKey(Long familyId, Long customerId) {
        return familyCustomerPrefix(familyId, customerId) + KEY_SEPARATOR + "constraints";
    }

    // policy 이벤트 dedup 키를 만든다.
    public String generatePolicyEventDedupKey(String eventId, Long customerId) {
        return POLICY_EVENT_DEDUP_KEY_PREFIX + KEY_SEPARATOR + eventId + KEY_SEPARATOR + customerId;
    }

    // usage 이벤트 dedup 키를 만든다.
    public String generateUsageEventDedupKey(String eventId) {
        return USAGE_EVENT_DEDUP_KEY_PREFIX + KEY_SEPARATOR + eventId;
    }

    // family-customer 공통 prefix를 만든다.
    private String familyCustomerPrefix(Long familyId, Long customerId) {
        return FAMILY_KEY_PREFIX
                + KEY_SEPARATOR
                + familyId
                + KEY_SEPARATOR
                + "customer"
                + KEY_SEPARATOR
                + customerId;
    }

    // 월 suffix를 공통 형식으로 만든다.
    private String formatMonth(LocalDate eventMonth) {
        return eventMonth.format(MONTH_SUFFIX_FORMATTER);
    }
}
