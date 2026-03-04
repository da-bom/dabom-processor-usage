package com.project.global.util;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.springframework.stereotype.Component;

@Component
public class RedisKeyGenerator {

    private static final String KEY_SEPARATOR = ":";
    private static final String FAMILY_KEY_PREFIX = "family";
    private static final String POLICY_EVENT_DEDUP_KEY_PREFIX = "event:dedup:policy";
    private static final DateTimeFormatter MONTH_SUFFIX_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMM");

    public String generateFamilyAlertsKey(Long familyId) {
        return FAMILY_KEY_PREFIX
                + KEY_SEPARATOR
                + familyId
                + KEY_SEPARATOR
                + "alert"
                + KEY_SEPARATOR
                + "THRESHOLD";
    }

    public String generateFamilyInfoKey(Long familyId) {
        return FAMILY_KEY_PREFIX + KEY_SEPARATOR + familyId + KEY_SEPARATOR + "info";
    }

    public String generateFamilyRemainingKey(Long familyId) {
        return FAMILY_KEY_PREFIX + KEY_SEPARATOR + familyId + KEY_SEPARATOR + "remaining";
    }

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

    public String generatePolicyEventDedupKey(String eventId, Long customerId) {
        return POLICY_EVENT_DEDUP_KEY_PREFIX + KEY_SEPARATOR + eventId + KEY_SEPARATOR + customerId;
    }
}
