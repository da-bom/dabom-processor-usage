package com.project.domain.policy.service.helper;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.regex.Pattern;

import org.springframework.stereotype.Component;

import com.project.domain.policy.constant.PolicyConstraintKeyConstants;
import com.project.global.event.dto.policy.PolicyUpdatedPayload;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class PolicyEventValidator {
    private static final Pattern HHMM_PATTERN = Pattern.compile("^\\d{4}$");
    private static final Pattern HHMM_RANGE_PATTERN = Pattern.compile("^\\d{4}-\\d{4}$");
    private static final Predicate<String> BINARY_FLAG_VALIDATOR =
            value -> "1".equals(value) || "0".equals(value);
    private static final Predicate<String> POSITIVE_LONG_VALIDATOR =
            PolicyEventValidator::isPositiveLongValue;
    private static final Predicate<String> HHMM_RANGE_VALIDATOR =
            PolicyEventValidator::isValidHhmmRangeValue;
    private static final Predicate<String> APP_LIST_VALIDATOR =
            PolicyEventValidator::isValidAppListValue;

    private static final Map<String, Predicate<String>> EXACT_VALUE_VALIDATORS =
            Map.of(
                    PolicyConstraintKeyConstants.BLOCK_ACCESS,
                    BINARY_FLAG_VALIDATOR,
                    PolicyConstraintKeyConstants.BLOCK_TIME,
                    HHMM_RANGE_VALIDATOR,
                    PolicyConstraintKeyConstants.BLOCK_APP,
                    APP_LIST_VALIDATOR);

    private static final Map<String, Predicate<String>> PREFIX_VALUE_VALIDATORS =
            Map.of(
                    PolicyConstraintKeyConstants.BLOCK_APP_PREFIX,
                    BINARY_FLAG_VALIDATOR,
                    "LIMIT:DATA:",
                    POSITIVE_LONG_VALIDATOR);

    public boolean isValidPayload(PolicyUpdatedPayload payload, String eventId, String recordKey) {
        if (payload == null) {
            log.warn("policy-updated payload is null. recordKey={}", recordKey);
            return false;
        }

        if (eventId == null || eventId.isBlank()) {
            log.warn(
                    "policy-updated eventId is empty. familyId={}, customerId={}, policyKey={}",
                    payload.familyId(),
                    payload.targetCustomerId(),
                    payload.policyKey());
            return false;
        }

        if (payload.familyId() == null
                || payload.policyKey() == null
                || payload.policyKey().isBlank()) {
            log.warn(
                    "Invalid policy-updated payload. eventId={}, familyId={}, customerId={},"
                            + " policyKey={}",
                    eventId,
                    payload.familyId(),
                    payload.targetCustomerId(),
                    payload.policyKey());
            return false;
        }

        return true;
    }

    public boolean isAllowedPolicyKey(String policyKey) {
        if (policyKey == null || policyKey.isBlank()) {
            return false;
        }
        if (EXACT_VALUE_VALIDATORS.containsKey(policyKey)) {
            return true;
        }
        return findPrefixValidator(policyKey) != null;
    }

    public boolean isValidPolicyValue(String policyKey, String newValue) {
        if (policyKey == null || policyKey.isBlank()) {
            return false;
        }
        Predicate<String> exactValidator = EXACT_VALUE_VALIDATORS.get(policyKey);
        if (exactValidator != null) {
            return exactValidator.test(newValue);
        }
        Predicate<String> prefixValidator = findPrefixValidator(policyKey);
        if (prefixValidator != null) {
            return prefixValidator.test(newValue);
        }
        return false;
    }

    private Predicate<String> findPrefixValidator(String policyKey) {
        for (Map.Entry<String, Predicate<String>> entry : PREFIX_VALUE_VALIDATORS.entrySet()) {
            if (policyKey.startsWith(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static boolean isPositiveLongValue(String value) {
        try {
            return Long.parseLong(value) > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static boolean isValidHhmmValue(String value) {
        if (!HHMM_PATTERN.matcher(value).matches()) {
            return false;
        }
        int hh = Integer.parseInt(value.substring(0, 2));
        int mm = Integer.parseInt(value.substring(2, 4));
        return hh >= 0 && hh <= 23 && mm >= 0 && mm <= 59;
    }

    private static boolean isValidHhmmRangeValue(String value) {
        if (!HHMM_RANGE_PATTERN.matcher(value).matches()) {
            return false;
        }
        String[] tokens = value.split("-", -1);
        if (tokens.length != 2) {
            return false;
        }
        return isValidHhmmValue(tokens[0]) && isValidHhmmValue(tokens[1]);
    }

    private static boolean isValidAppListValue(String value) {
        if (value == null || value.isBlank()) {
            return true;
        }

        Set<String> appIds =
                parseCsvValues(value).stream()
                        .filter(appId -> !appId.isBlank())
                        .collect(Collectors.toSet());
        return !appIds.isEmpty();
    }

    private static Set<String> parseCsvValues(String value) {
        return Arrays.stream(value.split(",")).map(String::trim).collect(Collectors.toSet());
    }
}
