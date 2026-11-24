import math
import time
from typing import Dict, Optional, Tuple
from redis.asyncio import Redis
from app.core.logging import get_logger

logger = get_logger(__name__)

REDIS_CONCURRENCY_PREFIX = "task_engine:concurrency:active:"
REDIS_RATELIMIT_PREFIX = "task_engine:ratelimit:window:"

# Default global active concurrency limits per job type
DEFAULT_JOB_CONCURRENCY_LIMITS: Dict[str, int] = {
    "model_training": 3,      # At most 3 training jobs simultaneously across the cluster
    "model_inference": 10,    # At most 10 inference jobs simultaneously
    "batch_scoring": 5,       # At most 5 batch scorings
    "sleep": 20,              # Default max
}

# Default rate limits: (allowed_calls, window_seconds)
DEFAULT_JOB_RATE_LIMITS: Dict[str, Tuple[int, float]] = {
    "model_training": (10, 1.0),   # At most 10 training submissions/second
    "model_inference": (50, 1.0),  # At most 50 inference submissions/second
    "batch_scoring": (20, 1.0),    # At most 20 batch scorings/second
}

# Lua script for atomically acquiring an active concurrency slot for a job type
LUA_ACQUIRE_CONCURRENCY_SLOT = """
local key = KEYS[1]
local job_id = ARGV[1]
local max_limit = tonumber(ARGV[2])

local current_count = redis.call('SCARD', key)
if current_count >= max_limit then
    return 0 -- Rejected: concurrency limit reached
end

redis.call('SADD', key, job_id)
return 1 -- Granted
"""

# Lua script for sliding window rate limiter
LUA_SLIDING_WINDOW_RATELIMIT = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local max_allowed = tonumber(ARGV[3])
local cutoff = now - window

-- Evict timestamps older than the sliding window
redis.call('ZREMRANGEBYSCORE', key, '-inf', cutoff)

local current_count = redis.call('ZCARD', key)
if current_count >= max_allowed then
    return 0 -- Rejected: rate limit exceeded
end

local seq = redis.call('INCR', key .. ':seq')
local member = tostring(now) .. ':' .. tostring(seq)
redis.call('ZADD', key, now, member)
redis.call('EXPIRE', key, math.ceil(window * 2))
redis.call('EXPIRE', key .. ':seq', math.ceil(window * 2))
return 1 -- Allowed
"""


class RateLimiter:
    """Manages active concurrency quotas and sliding window rate limits per job type."""

    def __init__(self, redis: Redis) -> None:
        self.redis = redis
        self._acquire_concurrency_script = self.redis.register_script(LUA_ACQUIRE_CONCURRENCY_SLOT)
        self._sliding_window_script = self.redis.register_script(LUA_SLIDING_WINDOW_RATELIMIT)

    async def acquire_concurrency_slot(
        self,
        job_type: str,
        job_id: str,
        custom_limit: Optional[int] = None,
    ) -> bool:
        """Attempts to atomically acquire an active concurrency slot for this job type.
        
        Returns True if granted, False if global concurrency limit is saturated.
        """
        max_limit = custom_limit or DEFAULT_JOB_CONCURRENCY_LIMITS.get(job_type, 100)
        key = f"{REDIS_CONCURRENCY_PREFIX}{job_type}"

        granted = await self._acquire_concurrency_script(
            keys=[key],
            args=[job_id, max_limit],
        )
        return bool(granted)

    async def release_concurrency_slot(self, job_type: str, job_id: str) -> None:
        """Releases the active concurrency slot when the job finishes."""
        key = f"{REDIS_CONCURRENCY_PREFIX}{job_type}"
        await self.redis.srem(key, job_id)

    async def get_active_concurrency(self, job_type: str) -> int:
        """Returns the number of currently running jobs for this job type."""
        key = f"{REDIS_CONCURRENCY_PREFIX}{job_type}"
        return await self.redis.scard(key)

    async def check_rate_limit(
        self,
        job_type: str,
        limit: Optional[int] = None,
        window_seconds: Optional[float] = None,
    ) -> bool:
        """Sliding-window rate limiter per job type across the cluster.
        
        Returns True if allowed, False if rate limit is exceeded.
        """
        default_limit, default_window = DEFAULT_JOB_RATE_LIMITS.get(job_type, (100, 1.0))
        actual_limit = limit or default_limit
        actual_window = window_seconds or default_window

        key = f"{REDIS_RATELIMIT_PREFIX}{job_type}"
        now_ts = time.time()

        allowed = await self._sliding_window_script(
            keys=[key],
            args=[now_ts, actual_window, actual_limit],
        )
        return bool(allowed)
