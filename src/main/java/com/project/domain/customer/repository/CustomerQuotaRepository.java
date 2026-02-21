package com.project.domain.customer.repository;

import java.time.LocalDate;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.project.domain.customer.entity.CustomerQuota;

public interface CustomerQuotaRepository extends JpaRepository<CustomerQuota, Long> {

    Optional<CustomerQuota> findTopByFamilyIdAndCustomerIdAndDeletedAtIsNullOrderByCurrentMonthDesc(
            Long familyId, Long customerId);

    @Modifying
    @Query(
            "update CustomerQuota c "
                    + "set c.monthlyUsedBytes = c.monthlyUsedBytes + :bytesUsed "
                    + "where c.familyId = :familyId "
                    + "and c.customerId = :customerId "
                    + "and c.currentMonth = :currentMonth "
                    + "and c.deletedAt is null")
    int incrementMonthlyUsedBytes(
            @Param("familyId") Long familyId,
            @Param("customerId") Long customerId,
            @Param("currentMonth") LocalDate currentMonth,
            @Param("bytesUsed") Long bytesUsed);

    @Query(
            """
        SELECT cq
        FROM CustomerQuota cq
        WHERE cq.familyId = :familyId
          AND cq.customerId = :customerId
          AND cq.currentMonth = :currentMonth
          AND cq.deletedAt IS NULL
        """)
    Optional<CustomerQuota> findActiveByFamilyIdAndCustomerIdAndCurrentMonth(
            @Param("familyId") Long familyId,
            @Param("customerId") Long customerId,
            @Param("currentMonth") LocalDate currentMonth);
}
