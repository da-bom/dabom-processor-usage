package com.project.global.metrics;

import java.util.Set;

public final class KafkaMetricTagSanitizer {
    public static final String UNKNOWN_EVENT_TYPE = "UNKNOWN";
    public static final String OTHER_EVENT_TYPE = "OTHER";
    private static final String DATA_USAGE = "DATA_USAGE";
    private static final String POLICY_UPDATED = "POLICY_UPDATED";
    private static final String USAGE_PERSIST = "USAGE_PERSIST";
    private static final String NOTIFICATION = "NOTIFICATION";
    private static final String USAGE_REALTIME = "USAGE_REALTIME";

    private static final Set<String> ALLOWED_EVENT_TYPES =
            Set.of(DATA_USAGE, POLICY_UPDATED, USAGE_PERSIST, NOTIFICATION, USAGE_REALTIME);

    private KafkaMetricTagSanitizer() {}

    public static String normalizeEventType(String rawEventType) {
        if (rawEventType == null || rawEventType.isBlank()) {
            return UNKNOWN_EVENT_TYPE;
        }
        if (ALLOWED_EVENT_TYPES.contains(rawEventType)) {
            return rawEventType;
        }
        return OTHER_EVENT_TYPE;
    }
}
