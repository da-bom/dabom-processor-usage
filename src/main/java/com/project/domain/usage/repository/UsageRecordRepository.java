package com.project.domain.usage.repository;

import java.time.LocalDateTime;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.project.domain.usage.entity.UsageRecord;

public interface UsageRecordRepository extends JpaRepository<UsageRecord, Long> {

    @Modifying
    @Query(
            value =
                    """
                    INSERT IGNORE INTO usage_record
                        (event_id, family_id, customer_id, bytes_used, app_id, event_time, created_at, updated_at)
                    VALUES
                        (:eventId, :familyId, :customerId, :bytesUsed, :appId, :eventTime, NOW(), NOW())
                    """,
            nativeQuery = true)
    int upsertUsageRecord(
            @Param("eventId") String eventId,
            @Param("familyId") Long familyId,
            @Param("customerId") Long customerId,
            @Param("bytesUsed") Long bytesUsed,
            @Param("appId") String appId,
            @Param("eventTime") LocalDateTime eventTime);
}
