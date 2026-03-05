-- KEYS[1]: family:{fid}:info
-- KEYS[2]: family:{fid}:remaining
-- KEYS[3]: family:{fid}:customer:{uid}:usage:monthly:{yyyyMM}
-- KEYS[4]: family:{fid}:customer:{uid}:constraints
-- KEYS[5]: family:{fid}:alert:THRESHOLD (prefix)
-- ARGV[1]: usageBytes
-- ARGV[2]: currentHHmm (e.g. 2230)

local usageBytes = tonumber(ARGV[1])
local currentHHmm = tonumber(ARGV[2] or '0')

-- 1. [Check] 개인 제약 조건 로딩
local constraints_array = redis.call('HGETALL', KEYS[4])
local constraints = {}
for i = 1, #constraints_array, 2 do
    constraints[constraints_array[i]] = constraints_array[i+1]
end

local monthlyLimitStr = constraints['LIMIT:DATA:MONTHLY']
local monthlyLimit = -1
if monthlyLimitStr then monthlyLimit = tonumber(monthlyLimitStr) end

local currentMonthly = tonumber(redis.call('GET', KEYS[3]) or '0')

-- (공통 반환 함수)
local function getResult(status, currentMonthlyUsed)
    local totalLimit = tonumber(redis.call('HGET', KEYS[1], 'totalQuota') or '0')
    local currentRemaining = tonumber(redis.call('GET', KEYS[2]) or totalLimit)
    local totalUsed = totalLimit - currentRemaining
    local userRatio = 0
    if totalLimit > 0 then userRatio = currentMonthlyUsed / totalLimit end

    return {totalUsed, currentRemaining, status, currentMonthlyUsed, userRatio, monthlyLimit}
end

-- 1. [Block] 완전 차단 여부
if constraints['BLOCK:ACCESS'] == "1" then
    return getResult("MANUAL", currentMonthly)
end

local blockStart = nil
local blockEnd = nil

-- BLOCK:TIME = "HHMM-HHMM" 해당 포멧을 기준으로 파싱
local blockTimeRange = constraints['BLOCK:TIME']
if blockTimeRange then
    local dashPos = string.find(blockTimeRange, "-", 1, true)
    if dashPos then
        local startStr = string.sub(blockTimeRange, 1, dashPos - 1)
        local endStr = string.sub(blockTimeRange, dashPos + 1)
        blockStart = tonumber(startStr)
        blockEnd = tonumber(endStr)
    end
end

-- 2. [Block] 시간 차단 여부
if blockStart and blockEnd then
    if blockStart < blockEnd then
        -- same-day window, e.g. 0900~1800
        if currentHHmm >= blockStart and currentHHmm < blockEnd then
            return getResult("TIME_BLOCK", currentMonthly)
        end
    elseif blockStart > blockEnd then
        -- overnight window, e.g. 2200~0700
        if currentHHmm >= blockStart or currentHHmm < blockEnd then
            return getResult("TIME_BLOCK", currentMonthly)
        end
    else
        -- start == end means full-day block
        return getResult("TIME_BLOCK", currentMonthly)
    end
end

-- 3. [Limit] 개인 월간 한도 초과 여부
if monthlyLimit ~= -1 then
    if (currentMonthly + usageBytes) > monthlyLimit then
        return getResult("MONTHLY_LIMIT_EXCEEDED", currentMonthly)
    end
end

-- 4. [Quota] 가족 잔여량 부족 여부
local currentRemaining = tonumber(redis.call('GET', KEYS[2]))
if currentRemaining == nil then
    local totalLimit = tonumber(redis.call('HGET', KEYS[1], 'totalQuota') or '0')
    currentRemaining = totalLimit
end

if currentRemaining < usageBytes then
    return getResult("FAMILY_QUOTA_EXCEEDED", currentMonthly)
end

-- 5. 가족 잔여량 차감
local newRemaining = redis.call('DECRBY', KEYS[2], usageBytes)

-- 6. 개인 월간 사용량 증가
local newMonthly = redis.call('INCRBY', KEYS[3], usageBytes)

-- 7. [Result] 상태 결정
local limitStr = redis.call('HGET', KEYS[1], 'totalQuota')
local totalLimit = tonumber(limitStr or '0')
local status = "NORMAL"

if newRemaining <= 0 then
    status = "FAMILY_QUOTA_EXCEEDED"
else
    local ratio = 0
    if totalLimit > 0 then
        ratio = newRemaining / totalLimit
    end

    -- 현재 미도달 경고 중 가장 작은 임계
    local alertLevel = nil
    if ratio < 0.1 then
        alertLevel = "10"
        status = "WARNING_10"
    elseif ratio < 0.3 then
        alertLevel = "30"
        status = "WARNING_30"
    elseif ratio < 0.5 then
        alertLevel = "50"
        status = "WARNING_50"
    end

    -- 경고 상태이면 중복 체크
    if alertLevel then
        local alertKey = KEYS[5] .. ":" .. alertLevel
        -- 이미 알림을 보냈는지 확인
        local isSent = redis.call('EXISTS', alertKey)

        if isSent == 1 then
            -- 이미 보낸 레벨이면 상태를 NORMAL로 덮어씀
            status = "NORMAL"
        else
            -- 아직 안보냈으면 알림 상태 기록 (PUBLISHED)
            redis.call('SET', alertKey, "PUBLISHED")
        end
    end
end

-- 8. 최종 반환
local totalUsed = totalLimit - newRemaining
local userRatio = 0
if totalLimit > 0 then userRatio = newMonthly / totalLimit end

return {totalUsed, newRemaining, status, newMonthly, userRatio, monthlyLimit}
