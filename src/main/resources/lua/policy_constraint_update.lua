-- policy_constraint_update.lua
--
-- Purpose:
--   Atomically apply one customer constraint update with dedup + stale-event guard.
--
-- KEYS
--   KEYS[1]: event:dedup:policy:{eventId}:{customerId}
--   KEYS[2]: family:{familyId}:customer:{customerId}:constraints
--   KEYS[3]: family:{familyId}:customer:{customerId}:constraints:version
--
-- ARGV
--   ARGV[1]: dedup_ttl_seconds
--   ARGV[2]: policy_key
--   ARGV[3]: new_value (blank => delete)
--   ARGV[4]: event_timestamp_epoch_millis
--
-- Return (array)
--   {"APPLIED", "HSET"|"HDEL"}
--   {"DUPLICATE"}
--   {"STALE", last_applied_version}
--   {"INVALID_REQUEST", reason}

local dedup_key = KEYS[1]
local constraints_key = KEYS[2]
local version_key = KEYS[3]

local dedup_ttl = tonumber(ARGV[1])
local policy_key = ARGV[2]
local new_value = ARGV[3]
local event_version = tonumber(ARGV[4])

if not dedup_ttl or dedup_ttl <= 0 then
    return {"INVALID_REQUEST", "INVALID_DEDUP_TTL"}
end

if not policy_key or policy_key == "" then
    return {"INVALID_REQUEST", "EMPTY_POLICY_KEY"}
end

if not event_version then
    return {"INVALID_REQUEST", "INVALID_EVENT_VERSION"}
end

local first_seen = redis.call("SET", dedup_key, "1", "NX", "EX", dedup_ttl)
if not first_seen then
    return {"DUPLICATE"}
end

local last_applied = tonumber(redis.call("HGET", version_key, policy_key) or "-1")
if event_version < last_applied then
    return {"STALE", tostring(last_applied)}
end

if not new_value or new_value == "" then
    redis.call("HDEL", constraints_key, policy_key)
    redis.call("HSET", version_key, policy_key, tostring(event_version))
    return {"APPLIED", "HDEL"}
end

redis.call("HSET", constraints_key, policy_key, new_value)
redis.call("HSET", version_key, policy_key, tostring(event_version))
return {"APPLIED", "HSET"}
