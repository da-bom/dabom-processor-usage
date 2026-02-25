package com.project.global.event.dto.usage;

import java.util.Map;

public record UsagePayload(
        Long familyId,
        Long customerId,
        String appId,
        Long bytesUsed,
        Map<String, Object> metadata) {}
