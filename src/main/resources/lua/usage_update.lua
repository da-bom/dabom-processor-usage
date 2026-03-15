-- KEYS[1]: family:{fid}:info:{yyyyMM}
-- KEYS[2]: family:{fid}:remaining:{yyyyMM}
-- KEYS[3]: family:{fid}:customer:{uid}:usage:monthly:{yyyyMM}
-- KEYS[4]: family:{fid}:customer:{uid}:constraints
-- KEYS[5]: family:{fid}:alert:THRESHOLD:50:{yyyyMM}
-- KEYS[6]: family:{fid}:alert:THRESHOLD:30:{yyyyMM}
-- KEYS[7]: family:{fid}:alert:THRESHOLD:10:{yyyyMM}
-- KEYS[8]: event:dedup:usage:{eventId}
-- ARGV[1]: usageBytes
-- ARGV[2]: currentHHmm (e.g. 2230)
-- ARGV[3]: normalizedAppId
-- ARGV[4]: dedupTtlSeconds

local usageBytes = tonumber(ARGV[1])
local currentHHmm = tonumber(ARGV[2] or '0')
local appId = ARGV[3] or ''
local dedupTtlSeconds = tonumber(ARGV[4] or '0')
local monthlyLimit = -1

-- 공통 반환 형식
local function getResult(status, currentMonthlyUsed, duplicate)
    local totalLimit = tonumber(redis.call('HGET', KEYS[1], 'totalQuota') or '0')
    local currentRemaining = tonumber(redis.call('GET', KEYS[2]) or totalLimit)
    local totalUsed = totalLimit - currentRemaining
    local userRatio = 0
    if totalLimit > 0 then
        userRatio = currentMonthlyUsed / totalLimit
    end

    return {
        totalUsed,
        currentRemaining,
        status,
        currentMonthlyUsed,
        userRatio,
        monthlyLimit,
        duplicate and 1 or 0
    }
end

-- 1) 동일 eventId 재처리 방지
if dedupTtlSeconds > 0 then
    -- 같은 eventId는 월별 상태 반영 전에 바로 차단함
    local firstSeen = redis.call('SET', KEYS[8], '1', 'NX', 'EX', dedupTtlSeconds)
    if not firstSeen then
        return {0, 0, 'DUPLICATE', 0, 0, -1, 1}
    end
end

-- 2) 고객별 제약 조회
local constraintsArray = redis.call('HGETALL', KEYS[4])
local constraints = {}
for i = 1, #constraintsArray, 2 do
    constraints[constraintsArray[i]] = constraintsArray[i + 1]
end

local monthlyLimitStr = constraints['LIMIT:DATA:MONTHLY']
if monthlyLimitStr then
    monthlyLimit = tonumber(monthlyLimitStr)
end

local currentMonthly = tonumber(redis.call('GET', KEYS[3]) or '0')

-- 3) 차단/제한 조건 확인
if constraints['BLOCK:ACCESS'] == '1' then
    return getResult('MANUAL', currentMonthly, false)
end

if appId ~= '' and constraints['BLOCK:APP:' .. appId] == '1' then
    return getResult('APP_BLOCK', currentMonthly, false)
end

local blockStart = nil
local blockEnd = nil

local blockTimeRange = constraints['BLOCK:TIME']
if blockTimeRange then
    local dashPos = string.find(blockTimeRange, '-', 1, true)
    if dashPos then
        local startStr = string.sub(blockTimeRange, 1, dashPos - 1)
        local endStr = string.sub(blockTimeRange, dashPos + 1)
        blockStart = tonumber(startStr)
        blockEnd = tonumber(endStr)
    end
end

if blockStart and blockEnd then
    if blockStart < blockEnd then
        if currentHHmm >= blockStart and currentHHmm < blockEnd then
            return getResult('TIME_BLOCK', currentMonthly, false)
        end
    elseif blockStart > blockEnd then
        if currentHHmm >= blockStart or currentHHmm < blockEnd then
            return getResult('TIME_BLOCK', currentMonthly, false)
        end
    else
        return getResult('TIME_BLOCK', currentMonthly, false)
    end
end

if monthlyLimit ~= -1 then
    if (currentMonthly + usageBytes) > monthlyLimit then
        return getResult('MONTHLY_LIMIT_EXCEEDED', currentMonthly, false)
    end
end

local currentRemaining = tonumber(redis.call('GET', KEYS[2]))
if currentRemaining == nil then
    -- 월초 첫 이벤트면 remaining이 아직 없을 수 있어서 totalQuota로 시작함
    local totalLimit = tonumber(redis.call('HGET', KEYS[1], 'totalQuota') or '0')
    currentRemaining = totalLimit
end

if currentRemaining < usageBytes then
    return getResult('FAMILY_QUOTA_EXCEEDED', currentMonthly, false)
end

-- 4) 사용량 반영
local newRemaining = redis.call('DECRBY', KEYS[2], usageBytes)
local newMonthly = redis.call('INCRBY', KEYS[3], usageBytes)

-- 5) 후속 상태 계산
local limitStr = redis.call('HGET', KEYS[1], 'totalQuota')
local totalLimit = tonumber(limitStr or '0')
local status = 'NORMAL'
local ratio = 0

if totalLimit > 0 then
    ratio = newRemaining / totalLimit
end

local alertKey = nil
if ratio < 0.1 then
    alertKey = KEYS[7]
    status = 'WARNING_10'
elseif ratio < 0.3 then
    alertKey = KEYS[6]
    status = 'WARNING_30'
elseif ratio < 0.5 then
    alertKey = KEYS[5]
    status = 'WARNING_50'
end

if alertKey then
    -- 같은 월 같은 임계치는 suffix key 존재 여부로 한 번만 발행함
    local isSent = redis.call('EXISTS', alertKey)
    if isSent == 1 then
        status = 'NORMAL'
    else
        redis.call('SET', alertKey, 'PUBLISHED')
    end
end

local totalUsed = totalLimit - newRemaining
local userRatio = 0
if totalLimit > 0 then
    userRatio = newMonthly / totalLimit
end

return {totalUsed, newRemaining, status, newMonthly, userRatio, monthlyLimit, 0}
