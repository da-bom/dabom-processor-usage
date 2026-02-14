package com.project.domain.notification.service.port;

import com.project.global.event.dto.notification.NotificationPayload;

public interface NotificationEventPublisher {

    void publish(NotificationPayload payload);
}
