package com.project.domain.usage.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.project.domain.usage.entity.UsageRecord;

public interface UsageRecordRepository extends JpaRepository<UsageRecord, Long> {}
