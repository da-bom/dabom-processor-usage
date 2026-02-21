-- KEYS[1]: family:{fid}:info
-- KEYS[2]: family:{fid}:remaining
-- KEYS[3]: family:{fid}:customer:{uid}:usage:monthly
-- KEYS[4]: family:{fid}:customer:{uid}:constraints
-- ARGV[1]: usageBytes

local usageBytes = tonumber(ARGV[1])

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

-- (공통 리턴 함수)
local function getResult(status, currentMonthlyUsed)
    local totalLimit = tonumber(redis.call('HGET', KEYS[1], 'total_quota') or '0')
    local currentRemaining = tonumber(redis.call('GET', KEYS[2]) or totalLimit)
    local totalUsed = totalLimit - currentRemaining
    local userRatio = 0
    if totalLimit > 0 then userRatio = currentMonthlyUsed / totalLimit end

    return {totalUsed, currentRemaining, status, currentMonthlyUsed, userRatio, monthlyLimit}
end

-- 1. [Block] 완전 차단 여부
if constraints['BLOCK:ACCESS'] == "1" then
    return getResult("BLOCKED_ACCESS", currentMonthly)
end

-- 2. [Limit] 개인 월간 한도 초과 여부
if monthlyLimit ~= -1 then
    if (currentMonthly + usageBytes) > monthlyLimit then
        return getResult("BLOCKED_LIMIT_MONTHLY", currentMonthly)
    end
end

-- 3. [Quota] 가족 잔여량 부족 여부
local currentRemaining = tonumber(redis.call('GET', KEYS[2]))
if currentRemaining == nil then
    local totalLimit = tonumber(redis.call('HGET', KEYS[1], 'total_quota') or '0')
    currentRemaining = totalLimit
end

if currentRemaining < usageBytes then
    return getResult("BLOCKED_FAMILY_QUOTA", currentMonthly)
end

-- 4. 가족 잔여량 차감
local newRemaining = redis.call('DECRBY', KEYS[2], usageBytes)

-- 5. 개인 월간 사용량 증가
local newMonthly = redis.call('INCRBY', KEYS[3], usageBytes)

-- 6. [Result] 상태 판정
local limitStr = redis.call('HGET', KEYS[1], 'total_quota')
local totalLimit = tonumber(limitStr or '0')
local status = "NORMAL"

if newRemaining <= 0 then
    status = "BLOCKED_FAMILY_QUOTA"
else
    local ratio = 0
    if totalLimit > 0 then
        ratio = newRemaining / totalLimit
    end

    -- 현재 도달한 경고 레벨 식별
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

    -- 경고 상태라면 중복 체크
    if alertLevel then
        local alertKey = "THRESHOLD:" .. alertLevel
        -- 이미 알림을 보냈는지 확인
        local isSent = redis.call('HEXISTS', KEYS[5], alertKey)

        if isSent == 1 then
            -- 이미 보냈으므로 상태를 NORMAL로 덮어씀
            status = "NORMAL"
        else
            -- 아직 안보냈으면 알림 상태 기록 (PUBLISHED)
            redis.call('HSET', KEYS[5], alertKey, "PUBLISHED")
        end
    end
end

-- 7. 최종 반환
local totalUsed = totalLimit - newRemaining
local userRatio = 0
if totalLimit > 0 then userRatio = newMonthly / totalLimit end

return {totalUsed, newRemaining, status, newMonthly, userRatio, monthlyLimit}