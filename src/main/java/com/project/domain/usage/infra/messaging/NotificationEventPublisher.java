package com.project.domain.usage.infra.messaging;

import com.project.global.event.dto.notification.NotificationPayload;

public interface NotificationEventPublisher {

    void publish(NotificationPayload payload);
}
