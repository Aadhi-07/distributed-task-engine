import time
from typing import Optional, List
from redis.asyncio import Redis

from app.core.logging import get_logger

logger = get_logger(__name__)

REDIS_PRIORITY_QUEUE_KEY = "task_engine:queue:priority"
REDIS_PROCESSING_HASH_KEY = "task_engine:processing"
REDIS_DELAYED_QUEUE_KEY = "task_engine:queue:delayed"
REDIS_DLQ_KEY = "task_engine:queue:dlq"
REDIS_CANCEL_PREFIX = "task_engine:cancel:"

# Lua script to atomically pop highest priority (lowest score) job
LUA_ATOMIC_CLAIM_JOB = """
local item = redis.call('ZPOPMIN', KEYS[1], 1)
if not item or #item == 0 then
    return nil
end
local job_id = item[1]
local score = item[2]
redis.call('HSET', KEYS[2], job_id, ARGV[1] .. ':' .. ARGV[2])
return job_id
"""

# Lua script to atomically complete processing
LUA_ATOMIC_COMPLETE_JOB = """
return redis.call('HDEL', KEYS[1], ARGV[1])
"""

# Lua script to atomically promote ready delayed jobs into the priority queue
LUA_PROMOTE_DELAYED_JOBS = """
local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
local count = 0
for i, item in ipairs(items) do
    redis.call('ZREM', KEYS[1], item)
    local sep_idx = string.find(item, ":")
    if sep_idx then
        local job_id = string.sub(item, 1, sep_idx - 1)
        local prio = tonumber(string.sub(item, sep_idx + 1)) or 4
        local inverted = 9 - prio
        local prio_score = (inverted * 1000000000.0) + (tonumber(ARGV[1]) % 1000000000.0)
        redis.call('ZADD', KEYS[2], prio_score, job_id)
        count = count + 1
    end
end
return count
"""


def compute_priority_score(priority: int, timestamp: Optional[float] = None) -> float:
    """Computes a 64-bit float score for Redis Sorted Set."""
    if timestamp is None:
        timestamp = time.time()
    bounded_priority = max(0, min(9, priority))
    inverted = 9 - bounded_priority
    return (inverted * 1_000_000_000.0) + (timestamp % 1_000_000_000.0)


class PriorityQueue:
    """Manages Redis sorted set priority queue, delayed retries, DLQ, and atomic worker claims."""

    def __init__(self, redis: Redis) -> None:
        self.redis = redis
        self._claim_script = self.redis.register_script(LUA_ATOMIC_CLAIM_JOB)
        self._complete_script = self.redis.register_script(LUA_ATOMIC_COMPLETE_JOB)
        self._promote_script = self.redis.register_script(LUA_PROMOTE_DELAYED_JOBS)

    async def qsize(self) -> int:
        """Returns the number of jobs currently waiting in the active priority queue."""
        return await self.redis.zcard(REDIS_PRIORITY_QUEUE_KEY)

    async def enqueue(
        self,
        job_id: str,
        priority: int,
        timestamp: Optional[float] = None,
    ) -> float:
        """Pushes a job into the active priority sorted set queue."""
        score = compute_priority_score(priority, timestamp)
        await self.redis.zadd(REDIS_PRIORITY_QUEUE_KEY, {job_id: score})
        return score

    async def enqueue_delayed(
        self,
        job_id: str,
        priority: int,
        delay_seconds: float,
    ) -> float:
        """Schedules a job for delayed retry execution."""
        ready_timestamp = time.time() + delay_seconds
        member = f"{job_id}:{priority}"
        await self.redis.zadd(REDIS_DELAYED_QUEUE_KEY, {member: ready_timestamp})
        return ready_timestamp

    async def promote_delayed_jobs(self, batch_size: int = 50) -> int:
        """Atomically moves due delayed retry jobs into the active priority queue."""
        now_ts = str(time.time())
        promoted_count = await self._promote_script(
            keys=[REDIS_DELAYED_QUEUE_KEY, REDIS_PRIORITY_QUEUE_KEY],
            args=[now_ts, batch_size],
        )
        return int(promoted_count or 0)

    async def send_to_dlq(self, job_id: str) -> None:
        """Moves exhausted job to the Dead Letter Queue (DLQ)."""
        await self.redis.zadd(REDIS_DLQ_KEY, {job_id: time.time()})

    async def mark_cancellation(self, job_id: str) -> None:
        """Sets a cancellation marker in Redis with 24-hour expiration."""
        cancel_key = f"{REDIS_CANCEL_PREFIX}{job_id}"
        await self.redis.set(cancel_key, "1", ex=86400)
        # Also remove from priority queue if still waiting
        await self.redis.zrem(REDIS_PRIORITY_QUEUE_KEY, job_id)

    async def is_cancelled(self, job_id: str) -> bool:
        """Checks if a job has received an active cancellation signal."""
        cancel_key = f"{REDIS_CANCEL_PREFIX}{job_id}"
        return bool(await self.redis.exists(cancel_key))

    async def clear_cancellation(self, job_id: str) -> None:
        cancel_key = f"{REDIS_CANCEL_PREFIX}{job_id}"
        await self.redis.delete(cancel_key)

    async def atomic_claim(self, worker_id: str) -> Optional[str]:
        """Atomically claims the next highest-priority job from the queue."""
        now_ts = str(time.time())
        claimed_id = await self._claim_script(
            keys=[REDIS_PRIORITY_QUEUE_KEY, REDIS_PROCESSING_HASH_KEY],
            args=[worker_id, now_ts],
        )
        if claimed_id:
            return claimed_id.decode("utf-8") if isinstance(claimed_id, bytes) else str(claimed_id)
        return None

    async def complete_processing(self, job_id: str) -> None:
        """Removes the job from the in-flight processing hash."""
        await self._complete_script(
            keys=[REDIS_PROCESSING_HASH_KEY],
            args=[job_id],
        )

    async def get_queue_depth(self) -> int:
        """Returns the number of waiting jobs in the priority queue."""
        return await self.redis.zcard(REDIS_PRIORITY_QUEUE_KEY)

    async def get_delayed_count(self) -> int:
        """Returns the number of jobs waiting in the delayed retry queue."""
        return await self.redis.zcard(REDIS_DELAYED_QUEUE_KEY)

    async def get_dlq_count(self) -> int:
        """Returns the number of jobs in the Dead Letter Queue."""
        return await self.redis.zcard(REDIS_DLQ_KEY)

    async def get_processing_count(self) -> int:
        """Returns the number of in-flight jobs currently being processed."""
        return await self.redis.hlen(REDIS_PROCESSING_HASH_KEY)
