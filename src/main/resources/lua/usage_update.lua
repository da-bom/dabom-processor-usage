-- KEYS[1]: family:{fid}:info:{yyyyMM}
-- KEYS[2]: family:{fid}:remaining:{yyyyMM}
-- KEYS[3]: family:{fid}:customer:{uid}:usage:monthly:{yyyyMM}
-- KEYS[4]: family:{fid}:customer:{uid}:constraints
-- KEYS[5]: family:{fid}:customer:{uid}:alert:THRESHOLD:50:{yyyyMM}
-- KEYS[6]: family:{fid}:customer:{uid}:alert:THRESHOLD:30:{yyyyMM}
-- KEYS[7]: family:{fid}:customer:{uid}:alert:THRESHOLD:10:{yyyyMM}
-- KEYS[8]: family:{fid}:customer:{uid}:alert:MANUAL:{yyyyMM}
-- KEYS[9]: family:{fid}:customer:{uid}:alert:APP_BLOCK:{appId}:{yyyyMM}
-- KEYS[10]: family:{fid}:customer:{uid}:alert:TIME_BLOCK:{yyyyMM}
-- KEYS[11]: family:{fid}:customer:{uid}:alert:MONTHLY_LIMIT_EXCEEDED:{yyyyMM}
-- KEYS[12]: family:{fid}:customer:{uid}:alert:FAMILY_QUOTA_EXCEEDED:{yyyyMM}
-- KEYS[13]: event:dedup:usage:{eventId}
-- ARGV[1]: usageBytes
-- ARGV[2]: currentHHmm (e.g. 2230)
-- ARGV[3]: normalizedAppId
-- ARGV[4]: dedupTtlSeconds

local STATUS_NORMAL = 'NORMAL'
local STATUS_MANUAL = 'MANUAL'
local STATUS_APP_BLOCK = 'APP_BLOCK'
local STATUS_TIME_BLOCK = 'TIME_BLOCK'
local STATUS_MONTHLY_LIMIT_EXCEEDED = 'MONTHLY_LIMIT_EXCEEDED'
local STATUS_FAMILY_QUOTA_EXCEEDED = 'FAMILY_QUOTA_EXCEEDED'
local STATUS_WARNING_50 = 'WARNING_50'
local STATUS_WARNING_30 = 'WARNING_30'
local STATUS_WARNING_10 = 'WARNING_10'
local STATUS_DUPLICATE = 'DUPLICATE'

local CONSTRAINT_BLOCK_ACCESS = 'BLOCK:ACCESS'
local CONSTRAINT_BLOCK_TIME = 'BLOCK:TIME'
local CONSTRAINT_LIMIT_DATA_MONTHLY = 'LIMIT:DATA:MONTHLY'
local CONSTRAINT_BLOCK_APP_PREFIX = 'BLOCK:APP:'

local ALERT_PUBLISHED = 'PUBLISHED'

local usageBytes = tonumber(ARGV[1])
local currentHHmm = tonumber(ARGV[2] or '0')
local appId = ARGV[3] or ''
local dedupTtlSeconds = tonumber(ARGV[4] or '0')
local monthlyLimit = -1

-- 잘못된 입력은 Redis를 건드리기 전에 즉시 중단한다.
if not usageBytes or usageBytes <= 0 then
    return redis.error_reply('usageBytes must be a positive number')
end

if not currentHHmm then
    return redis.error_reply('currentHHmm must be a number')
end

-- Lua 반환 형식에 맞는 결과 배열을 만든다.
local function build_result(status, currentMonthlyUsed, duplicate, currentRemaining, shouldNotify)
    local totalLimit = tonumber(redis.call('HGET', KEYS[1], 'totalQuota') or '0')
    local remaining = currentRemaining
    if remaining == nil then
        remaining = tonumber(redis.call('GET', KEYS[2]) or totalLimit)
    end

    local totalUsed = totalLimit - remaining
    local userRatio = 0
    if totalLimit > 0 then
        userRatio = currentMonthlyUsed / totalLimit
    end

    return {
        totalUsed,
        remaining,
        status,
        currentMonthlyUsed,
        userRatio,
        monthlyLimit,
        shouldNotify and 1 or 0,
        duplicate and 1 or 0
    }
end

-- dedup 캐시 문자열을 Lua 결과 형식으로 복원한다.
local function decode_cached_result(raw)
    if not raw then
        return nil
    end

    local parts = {}
    for token in string.gmatch(raw, "[^|]+") do
        table.insert(parts, token)
    end

    if #parts ~= 7 then
        return nil
    end

    return {
        tonumber(parts[1]) or 0,
        tonumber(parts[2]) or 0,
        parts[3] or STATUS_DUPLICATE,
        tonumber(parts[4]) or 0,
        tonumber(parts[5]) or 0,
        tonumber(parts[6]) or -1,
        tonumber(parts[7]) or 0,
        1
    }
end

-- Lua 결과를 dedup 캐시 문자열로 직렬화한다.
local function encode_cached_result(result)
    return table.concat({
        tostring(result[1]),
        tostring(result[2]),
        tostring(result[3]),
        tostring(result[4]),
        tostring(result[5]),
        tostring(result[6]),
        tostring(result[7])
    }, '|')
end

-- dedup TTL이 켜져 있으면 현재 결과를 캐시에 보관한다.
local function cache_result(result)
    if dedupTtlSeconds <= 0 then
        return
    end

    redis.call('SET', KEYS[13], encode_cached_result(result), 'EX', dedupTtlSeconds)
end

-- 차단 또는 제한으로 종료하는 결과를 캐시에 넣고 반환한다.
local function finalize(status, currentMonthlyUsed, currentRemaining, shouldNotify)
    local result = build_result(status, currentMonthlyUsed, false, currentRemaining, shouldNotify)
    cache_result(result)
    return result
end

-- 알림 key가 이미 있으면 재발행을 막고, 없으면 이번 달 최초 알림으로 기록한다.
local function consume_alert(key)
    local isSent = redis.call('EXISTS', key)
    if isSent == 1 then
        return false
    end

    redis.call('SET', key, ALERT_PUBLISHED)
    return true
end

-- 제약 조건 해시를 Lua 테이블로 읽어온다.
local function load_constraints()
    local constraintsArray = redis.call('HGETALL', KEYS[4])
    local constraints = {}
    for i = 1, #constraintsArray, 2 do
        constraints[constraintsArray[i]] = constraintsArray[i + 1]
    end
    return constraints
end

-- 월 한도 값을 읽고 없으면 제한 없음으로 본다.
local function resolve_monthly_limit(constraints)
    local monthlyLimitStr = constraints[CONSTRAINT_LIMIT_DATA_MONTHLY]
    if monthlyLimitStr then
        return tonumber(monthlyLimitStr)
    end
    return -1
end

-- 현재 가족 잔여량을 읽고 없으면 totalQuota로 복원한다.
local function resolve_current_remaining()
    local currentRemaining = tonumber(redis.call('GET', KEYS[2]))
    if currentRemaining ~= nil then
        return currentRemaining
    end

    local totalLimit = tonumber(redis.call('HGET', KEYS[1], 'totalQuota') or '0')
    return totalLimit
end

-- 시간 차단 규칙과 현재 시각의 충돌 여부를 확인한다.
local function resolve_time_block_status(blockTimeRange, hhmm)
    if not blockTimeRange then
        return nil
    end

    local dashPos = string.find(blockTimeRange, '-', 1, true)
    if not dashPos then
        return nil
    end

    local startStr = string.sub(blockTimeRange, 1, dashPos - 1)
    local endStr = string.sub(blockTimeRange, dashPos + 1)
    local blockStart = tonumber(startStr)
    local blockEnd = tonumber(endStr)

    if not blockStart or not blockEnd then
        return nil
    end

    if blockStart < blockEnd then
        if hhmm >= blockStart and hhmm < blockEnd then
            return STATUS_TIME_BLOCK
        end
        return nil
    end

    if blockStart > blockEnd then
        if hhmm >= blockStart or hhmm < blockEnd then
            return STATUS_TIME_BLOCK
        end
        return nil
    end

    return STATUS_TIME_BLOCK
end

-- 차단 또는 제한 조건을 우선순서대로 평가해 즉시 종료 상태를 결정한다.
local function resolve_block_status(
    constraints,
    normalizedAppId,
    hhmm,
    currentMonthlyUsed,
    requestBytes,
    currentRemaining
)
    if constraints[CONSTRAINT_BLOCK_ACCESS] == '1' then
        return STATUS_MANUAL, KEYS[8]
    end

    if normalizedAppId ~= '' and constraints[CONSTRAINT_BLOCK_APP_PREFIX .. normalizedAppId] == '1' then
        return STATUS_APP_BLOCK, KEYS[9]
    end

    local timeBlockStatus = resolve_time_block_status(constraints[CONSTRAINT_BLOCK_TIME], hhmm)
    if timeBlockStatus then
        return timeBlockStatus, KEYS[10]
    end

    if monthlyLimit ~= -1 and (currentMonthlyUsed + requestBytes) > monthlyLimit then
        return STATUS_MONTHLY_LIMIT_EXCEEDED, KEYS[11]
    end

    if currentRemaining < requestBytes then
        return STATUS_FAMILY_QUOTA_EXCEEDED, KEYS[12]
    end

    return nil, nil
end

-- Redis 반영 후 경고 상태와 알림 dedup 상태를 계산한다.
local function resolve_alert_status(newRemaining, newMonthly)
    local totalLimit = tonumber(redis.call('HGET', KEYS[1], 'totalQuota') or '0')
    if totalLimit <= 0 then
        return build_result(STATUS_NORMAL, newMonthly, false, newRemaining, false)
    end

    local ratio = newRemaining / totalLimit
    local alertKey = nil
    local status = STATUS_NORMAL

    if ratio < 0.1 then
        alertKey = KEYS[7]
        status = STATUS_WARNING_10
    elseif ratio < 0.3 then
        alertKey = KEYS[6]
        status = STATUS_WARNING_30
    elseif ratio < 0.5 then
        alertKey = KEYS[5]
        status = STATUS_WARNING_50
    end

    if not alertKey then
        return build_result(status, newMonthly, false, newRemaining, false)
    end

    return build_result(status, newMonthly, false, newRemaining, consume_alert(alertKey))
end

-- 같은 eventId 결과가 캐시에 남아 있으면 그대로 재사용한다.
if dedupTtlSeconds > 0 then
    local cached = decode_cached_result(redis.call('GET', KEYS[13]))
    if cached then
        return cached
    end
end

local constraints = load_constraints()
monthlyLimit = resolve_monthly_limit(constraints)

local currentMonthly = tonumber(redis.call('GET', KEYS[3]) or '0')
local currentRemaining = resolve_current_remaining()

-- 차단 또는 제한 조건이면 Redis 증감 없이 현재 상태만 반환한다.
local blockedStatus, blockedAlertKey =
        resolve_block_status(
                constraints, appId, currentHHmm, currentMonthly, usageBytes, currentRemaining)
if blockedStatus then
    return finalize(blockedStatus, currentMonthly, currentRemaining, consume_alert(blockedAlertKey))
end

-- 허용인 경우에만 Redis 사용량을 실제로 반영한다.
local newRemaining = redis.call('DECRBY', KEYS[2], usageBytes)
local newMonthly = redis.call('INCRBY', KEYS[3], usageBytes)

-- 반영 후 경고 상태를 계산하고 결과를 캐시해 둔다.
local finalResult = resolve_alert_status(newRemaining, newMonthly)
cache_result(finalResult)
return finalResult
