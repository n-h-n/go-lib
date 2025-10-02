package rlock

import "fmt"

var (
	concurrentLockLS string = fmt.Sprintf(`
		local lockKey = KEYS[1]
		local appID = ARGV[1]
		local maxInstances = tonumber(ARGV[2])
		local ttl = tonumber(ARGV[3])
		local lockType = ARGV[4]

		local readLockKey = lockKey .. ":%s"
		local writeLockKey = lockKey .. ":%s"
		
		if lockType == "read" then
			local writeLockExists = redis.call("SCARD", writeLockKey) > 0
			if writeLockExists then
				local remainingTTL = redis.call("TTL", writeLockKey)
				return {"blocked", remainingTTL}
			else
				if maxInstances == -1 then
					-- Unlimited concurrency for read locks
					redis.call("SADD", readLockKey, appID)
					redis.call("EXPIRE", readLockKey, ttl)
					return {"acquired", ttl}
				else
					local numReadInstances = redis.call("SCARD", readLockKey)
					if numReadInstances < maxInstances then
						local acquired = redis.call("SADD", readLockKey, appID)
						if acquired == 1 then
							redis.call("EXPIRE", readLockKey, ttl)
							return {"acquired", ttl}
						else
							local remainingTTL = redis.call("TTL", readLockKey)
							return {"blocked", remainingTTL}
						end
					else
						local remainingTTL = redis.call("TTL", readLockKey)
						return {"max_concurrency", remainingTTL}
					end
				end
			end
		elseif lockType == "write" then
			local readLockExists = redis.call("SCARD", readLockKey) > 0
			if readLockExists then
				local remainingTTL = redis.call("TTL", readLockKey)
				return {"blocked", remainingTTL}
			else
				if maxInstances == -1 then
					-- Unlimited concurrency for write locks
					redis.call("SADD", writeLockKey, appID)
					redis.call("EXPIRE", writeLockKey, ttl)
					return {"acquired", ttl}
				else
					local numWriteInstances = redis.call("SCARD", writeLockKey)
					if numWriteInstances < maxInstances then
						local acquired = redis.call("SADD", writeLockKey, appID)
						if acquired == 1 then
							redis.call("EXPIRE", writeLockKey, ttl)
							return {"acquired", ttl}
						else
							local remainingTTL = redis.call("TTL", writeLockKey)
							return {"blocked", remainingTTL}
						end
					else
						local remainingTTL = redis.call("TTL", writeLockKey)
						local instances = redis.call("SMEMBERS", writeLockKey)
						return {"max_concurrency", remainingTTL, instances}
					end
				end
			end
		else
			return {"unknown_lock_type"}
		end
	 `,
		LockTypeRead,
		LockTypeWrite,
	)
)
