package com.project.example.core.event.dto.usage;

import java.util.Map;

public record UsagePayload(
        String eventId,
        Long familyId,
        Long userId,
        String appId,
        Long bytesUsed,
        Map<String, Object> metadata // networkType, deviceId 등
        ) {}
