package com.project.domain.usage.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.project.domain.usage.entity.UsageEventOutbox;
import com.project.domain.usage.enums.UsageOutboxStatus;

public interface UsageEventOutboxRepository extends JpaRepository<UsageEventOutbox, Long> {

    Optional<UsageEventOutbox> findByEventId(String eventId);

    List<UsageEventOutbox> findByEventIdOrderByIdAsc(String eventId);

    boolean existsByEventIdAndStatus(String eventId, UsageOutboxStatus status);

    @Modifying
    @Query(
            value =
                    """
                    insert ignore into usage_event_outbox
                    (event_id, family_id, customer_id, status, retry_count, created_at, updated_at)
                    values (:eventId, :familyId, :customerId, 'PREPARED', 0, now(), now())
                    """,
            nativeQuery = true)
    int insertPreparedIgnore(
            @Param("eventId") String eventId,
            @Param("familyId") long familyId,
            @Param("customerId") long customerId);

    @Modifying
    @Query(
            """
            update UsageEventOutbox o
            set o.status = com.project.domain.usage.enums.UsageOutboxStatus.PUBLISH_PENDING,
                o.payloadJson = :payloadJson,
                o.nextRetryAt = null,
                o.lastError = null
            where o.eventId = :eventId
              and o.status = com.project.domain.usage.enums.UsageOutboxStatus.PREPARED
            """)
    int markPublishPendingIfPrepared(
            @Param("eventId") String eventId, @Param("payloadJson") String payloadJson);

    @Modifying
    @Query(
            """
            update UsageEventOutbox o
            set o.status = com.project.domain.usage.enums.UsageOutboxStatus.SKIPPED,
                o.payloadJson = null,
                o.nextRetryAt = null,
                o.lastError = null
            where o.eventId = :eventId
              and o.status = com.project.domain.usage.enums.UsageOutboxStatus.PREPARED
            """)
    int markSkippedIfPrepared(@Param("eventId") String eventId);

    @Modifying
    @Query(
            """
            update UsageEventOutbox o
            set o.status = com.project.domain.usage.enums.UsageOutboxStatus.SENT,
                o.nextRetryAt = null,
                o.lastError = null
            where o.id = :outboxId
              and o.status = com.project.domain.usage.enums.UsageOutboxStatus.PUBLISH_PENDING
            """)
    int markSentIfPending(@Param("outboxId") Long outboxId);
}
