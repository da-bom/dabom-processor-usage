package com.project.domain.policy.service.helper;

import org.springframework.stereotype.Component;

import com.project.domain.policy.constant.PolicyConstraintKeyConstants;
import com.project.global.event.dto.policy.PolicyUpdatedPayload;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class PolicyEventValidator {
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

        if (payload.policyKey() == null || payload.policyKey().isBlank()) {
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
        return PolicyConstraintKeyConstants.BLOCK_ACCESS.equals(policyKey)
                || PolicyConstraintKeyConstants.BLOCK_TIME.equals(policyKey)
                || PolicyConstraintKeyConstants.BLOCK_APP.equals(policyKey)
                || PolicyConstraintKeyConstants.LIMIT_DATA_MONTHLY.equals(policyKey)
                || policyKey.startsWith(PolicyConstraintKeyConstants.BLOCK_APP_PREFIX);
    }
}
