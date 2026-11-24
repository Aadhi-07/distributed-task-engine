import time
from typing import Optional, Tuple
from redis.asyncio import Redis

from app.core.logging import get_logger

logger = get_logger(__name__)

REDIS_PRIORITY_QUEUE_KEY = "task_engine:queue:priority"
REDIS_PROCESSING_HASH_KEY = "task_engine:processing"

# Lua script to atomically pop the highest priority (lowest score) job
# and move it to the in-flight processing hash with worker ID & start timestamp.
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

# Lua script to atomically remove the job from the in-flight processing hash.
LUA_ATOMIC_COMPLETE_JOB = """
return redis.call('HDEL', KEYS[1], ARGV[1])
"""


def compute_priority_score(priority: int, timestamp: Optional[float] = None) -> float:
    """Computes a 64-bit float score for Redis Sorted Set.
    
    Priorities range from 0 (lowest) to 9 (highest).
    Score calculation uses ZPOPMIN:
    - Inverted priority (9 - priority) forms the high-order magnitude (0 for prio 9, 9 for prio 0).
    - Monotonic epoch timestamp mod 1e9 forms the low-order magnitude.
    - Result: Priority 9 jobs always pop before Priority 8..0.
    - Within equal priority, earlier timestamps pop first (strict FIFO).
    """
    if timestamp is None:
        timestamp = time.time()
    bounded_priority = max(0, min(9, priority))
    inverted = 9 - bounded_priority
    return (inverted * 1_000_000_000.0) + (timestamp % 1_000_000_000.0)


class PriorityQueue:
    """Manages Redis sorted set priority queue and atomic worker claims."""

    def __init__(self, redis: Redis) -> None:
        self.redis = redis
        self._claim_script = self.redis.register_script(LUA_ATOMIC_CLAIM_JOB)
        self._complete_script = self.redis.register_script(LUA_ATOMIC_COMPLETE_JOB)

    async def enqueue(
        self,
        job_id: str,
        priority: int,
        timestamp: Optional[float] = None,
    ) -> float:
        """Pushes a job into the priority sorted set queue."""
        score = compute_priority_score(priority, timestamp)
        await self.redis.zadd(REDIS_PRIORITY_QUEUE_KEY, {job_id: score})
        return score

    async def atomic_claim(self, worker_id: str) -> Optional[str]:
        """Atomically claims the next highest-priority job from the queue.
        
        Guarantees that no two workers can claim the same job simultaneously.
        Returns the job_id string, or None if queue is empty.
        """
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

    async def get_processing_count(self) -> int:
        """Returns the number of in-flight jobs currently being processed."""
        return await self.redis.hlen(REDIS_PROCESSING_HASH_KEY)
