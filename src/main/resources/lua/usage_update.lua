-- KEYS[1]: family:{fid}:info (Hash)
-- KEYS[2]: family:{fid}:remaining (String)
-- ARGV[1]: usageBytes

local infoKey = KEYS[1]
local remainingKey = KEYS[2]
local usageBytes = tonumber(ARGV[1])

-- 1. 전체 할당량 조회
local limitStr = redis.call('HGET', infoKey, 'total_quota')
local totalLimit = tonumber(limitStr or '0')

-- 2. 현재 잔여량 조회
local currentRemaining = tonumber(redis.call('GET', remainingKey))
if currentRemaining == nil then
    currentRemaining = totalLimit
end

-- 3. 잔여량 차감
local newRemaining = redis.call('DECRBY', remainingKey, usageBytes)

-- 4. 상태 판정
local status = "NORMAL"

if newRemaining <= 0 then
    status = "BLOCKED"
else
    -- 남은 비율 계산
    local ratio = newRemaining / totalLimit

    if ratio < 0.1 then       -- 10% 미만
        status = "WARNING_10"
    elseif ratio < 0.3 then   -- 30% 미만
        status = "WARNING_30"
    elseif ratio < 0.5 then   -- 50% 미만
        status = "WARNING_50"
    end
end

-- 5. 총 사용량 계산
local totalUsed = totalLimit - newRemaining

return {totalUsed, newRemaining, status}