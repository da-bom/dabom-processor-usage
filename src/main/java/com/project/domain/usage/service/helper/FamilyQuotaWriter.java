package com.project.domain.usage.service.helper;

import java.time.LocalDate;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import com.project.domain.family.entity.FamilyQuota;
import com.project.domain.family.repository.FamilyQuotaRepository;
import com.project.global.exception.ApplicationException;
import com.project.global.exception.code.FamilyErrorCode;
import com.project.global.util.LogSanitizer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class FamilyQuotaWriter {
    private static final String FAMILY_QUOTA_LOG_SUFFIX =
            " familyId={}, bytesUsed={}, currentMonth={}";

    private final FamilyQuotaRepository familyQuotaRepository;
    private final LogSanitizer logSanitizer;

    public void persistAllowedQuota(
            Long familyId,
            LocalDate currentMonth,
            Long bytesUsed,
            String eventId,
            String originEventId) {
        // 현재 월 row가 이미 있으면 update로 끝냄
        if (tryUpdateExistingQuota(familyId, currentMonth, bytesUsed, eventId, originEventId)) {
            return;
        }
        // 현재 월 row가 없을 때만 최신 스냅샷을 이어받아 생성함
        createQuotaRow(familyId, currentMonth, bytesUsed, eventId, originEventId);
    }

    private boolean tryUpdateExistingQuota(
            Long familyId,
            LocalDate currentMonth,
            Long bytesUsed,
            String eventId,
            String originEventId) {
        int updatedRows =
                familyQuotaRepository.incrementUsedBytes(familyId, currentMonth, bytesUsed);
        if (updatedRows <= 0) {
            return false;
        }

        log.info(
                "Persisted usage to existing family_quota row. eventId={}, originEventId={},"
                        + FAMILY_QUOTA_LOG_SUFFIX,
                logSanitizer.sanitize(eventId),
                logSanitizer.sanitize(originEventId),
                familyId,
                bytesUsed,
                currentMonth);
        return true;
    }

    private void createQuotaRow(
            Long familyId,
            LocalDate currentMonth,
            Long bytesUsed,
            String eventId,
            String originEventId) {
        FamilyQuota latestSnapshot =
                familyQuotaRepository.findLatestByFamilyIdForUpdate(familyId).orElse(null);
        if (latestSnapshot == null) {
            log.warn(
                    "Missing latest family_quota snapshot. eventId={}, originEventId={},"
                            + FAMILY_QUOTA_LOG_SUFFIX,
                    logSanitizer.sanitize(eventId),
                    logSanitizer.sanitize(originEventId),
                    familyId,
                    bytesUsed,
                    currentMonth);
            throw new ApplicationException(FamilyErrorCode.LATEST_QUOTA_SNAPSHOT_NOT_FOUND);
        }

        // 다른 트랜잭션이 현재 월 row를 먼저 만들었으면 update 재시도로 수렴함
        if (currentMonth.equals(latestSnapshot.getCurrentMonth())) {
            if (tryUpdateExistingQuota(familyId, currentMonth, bytesUsed, eventId, originEventId)) {
                return;
            }
            throw new ApplicationException(FamilyErrorCode.FAMILY_QUOTA_UPDATE_FAILED);
        }

        FamilyQuota familyQuota =
                FamilyQuota.builder()
                        .familyId(familyId)
                        .currentMonth(currentMonth)
                        .totalQuotaBytes(latestSnapshot.getTotalQuotaBytes())
                        .usedBytes(bytesUsed)
                        .build();

        try {
            // 최신 totalQuotaBytes를 복사해서 새 월 row를 엶
            familyQuotaRepository.saveAndFlush(familyQuota);
        } catch (DataIntegrityViolationException e) {
            // insert 경합이면 update 재시도로 복구함
            if (tryUpdateExistingQuota(familyId, currentMonth, bytesUsed, eventId, originEventId)) {
                log.info(
                        "Recovered from concurrent family_quota insert race. eventId={},"
                                + " originEventId={},"
                                + FAMILY_QUOTA_LOG_SUFFIX,
                        logSanitizer.sanitize(eventId),
                        logSanitizer.sanitize(originEventId),
                        familyId,
                        bytesUsed,
                        currentMonth);
                return;
            }
            throw e;
        }

        log.info(
                "Persisted usage by creating family_quota row. eventId={}, originEventId={},"
                        + FAMILY_QUOTA_LOG_SUFFIX,
                logSanitizer.sanitize(eventId),
                logSanitizer.sanitize(originEventId),
                familyId,
                bytesUsed,
                currentMonth);
    }
}
