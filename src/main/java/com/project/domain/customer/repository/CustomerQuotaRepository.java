package com.project.domain.customer.repository;

import java.time.LocalDate;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.project.domain.customer.entity.CustomerQuota;

public interface CustomerQuotaRepository extends JpaRepository<CustomerQuota, Long> {

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
}
