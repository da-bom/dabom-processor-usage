package com.project.global.util;

import org.springframework.stereotype.Component;

@Component
public class RedisKeyGenerator {

    private static final String KEY_SEPARATOR = ":";
    private static final String EXAMPLE_KEY_PREFIX = "example";
    private static final String FAMILY_KEY_PREFIX = "family";

    public String generateExampleKey(Long exampleId) {
        return EXAMPLE_KEY_PREFIX + KEY_SEPARATOR + exampleId;
    }

    public String generateFamilyInfoKey(Long familyId) {
        return FAMILY_KEY_PREFIX + KEY_SEPARATOR + familyId + KEY_SEPARATOR + "info";
    }

    public String generateFamilyRemainingKey(Long familyId) {
        return FAMILY_KEY_PREFIX + KEY_SEPARATOR + familyId + KEY_SEPARATOR + "remaining";
    }

    public String generateFamilyCustomerMonthlyUsageKey(Long familyId, Long customerId) {
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
                + "monthly";
    }

    public String generateFamilyKey(Long familyId) {
        return FAMILY_KEY_PREFIX + KEY_SEPARATOR + familyId;
    }
}
