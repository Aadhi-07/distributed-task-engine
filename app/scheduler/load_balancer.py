import time
from typing import Any, Dict, List, Optional, Tuple
from redis.asyncio import Redis
from app.core.logging import get_logger

logger = get_logger(__name__)

REDIS_WORKERS_LOAD_ZSET = "task_engine:workers:load"
REDIS_WORKER_PRIVATE_QUEUE_PREFIX = "task_engine:worker:queue:"


def compute_worker_load_score(
    cpu_percent: float,
    memory_percent: float,
    running_jobs: int,
    concurrency: int,
) -> float:
    """Calculates a composite normalized load score between 0.0 and 100.0+.
    
    Weights:
    - 50% Concurrency utilization: (running_jobs / concurrency) * 100
    - 30% CPU utilization
    - 20% Memory utilization
    
    Lower scores indicate more available capacity.
    """
    concurrency_util = (running_jobs / max(1, concurrency)) * 100.0
    composite = (0.50 * concurrency_util) + (0.30 * cpu_percent) + (0.20 * memory_percent)
    return round(composite, 2)


class LoadBalancer:
    """Tracks cluster worker loads and routes jobs to the least-loaded nodes."""

    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    async def update_worker_load(
        self,
        worker_id: str,
        cpu_percent: float,
        memory_percent: float,
        running_jobs: int,
        concurrency: int,
    ) -> float:
        """Records current worker load score in Redis ZSET."""
        score = compute_worker_load_score(
            cpu_percent=cpu_percent,
            memory_percent=memory_percent,
            running_jobs=running_jobs,
            concurrency=concurrency,
        )
        await self.redis.zadd(REDIS_WORKERS_LOAD_ZSET, {worker_id: score})
        return score

    async def remove_worker(self, worker_id: str) -> None:
        """Removes a dead or stopped worker from the load balancing registry."""
        await self.redis.zrem(REDIS_WORKERS_LOAD_ZSET, worker_id)

    async def get_least_loaded_worker(
        self,
        max_acceptable_load: float = 90.0,
    ) -> Optional[Tuple[str, float]]:
        """Returns (worker_id, load_score) of the least-loaded worker with capacity.
        
        Returns None if no active workers or if all workers exceed max_acceptable_load.
        Distributes evenly across workers tied for minimum load to prevent starvation.
        """
        # Fetch up to 10 lowest-loaded workers
        candidates = await self.redis.zrange(
            REDIS_WORKERS_LOAD_ZSET, 0, 9, withscores=True
        )
        if not candidates:
            return None

        eligible: List[Tuple[str, float]] = []
        for worker_id_bytes, score in candidates:
            w_id = worker_id_bytes.decode() if isinstance(worker_id_bytes, bytes) else str(worker_id_bytes)
            if score <= max_acceptable_load:
                eligible.append((w_id, float(score)))

        if not eligible:
            # Fallback to absolute lowest if all are elevated
            best_id, best_score = candidates[0]
            w_id = best_id.decode() if isinstance(best_id, bytes) else str(best_id)
            return w_id, float(best_score)

        # Find all workers tied for minimum load score (within epsilon)
        min_score = eligible[0][1]
        tied = [c for c in eligible if abs(c[1] - min_score) < 0.01]

        if len(tied) == 1:
            return tied[0]

        # Round-robin among tied workers to avoid repeatedly starving other idle nodes
        rr_key = "task_engine:lb:round_robin"
        rr_index = await self.redis.incr(rr_key)
        chosen = tied[rr_index % len(tied)]
        return chosen

    async def assign_to_worker(self, worker_id: str, job_id: str) -> None:
        """Pushes a job into a targeted worker's private priority queue and tracks pending load."""
        queue_key = f"{REDIS_WORKER_PRIVATE_QUEUE_PREFIX}{worker_id}"
        await self.redis.rpush(queue_key, job_id)
        # Increment worker's load score in Redis ZSET to reflect the pending assignment
        await self.redis.zincrby(REDIS_WORKERS_LOAD_ZSET, 5.0, worker_id)

    async def pop_from_private_queue(self, worker_id: str) -> Optional[str]:
        """Pulls a job from the worker's dedicated private queue and marks it as in-flight."""
        queue_key = f"{REDIS_WORKER_PRIVATE_QUEUE_PREFIX}{worker_id}"
        now_ts = str(time.time())
        script = """
        local job_id = redis.call('LPOP', KEYS[1])
        if not job_id then
            return nil
        end
        redis.call('HSET', KEYS[2], job_id, ARGV[1] .. ':' .. ARGV[2])
        return job_id
        """
        job_id = await self.redis.eval(
            script,
            2,
            queue_key,
            "task_engine:processing",
            worker_id,
            now_ts,
        )
        if job_id:
            return job_id.decode() if isinstance(job_id, bytes) else str(job_id)
        return None

    async def get_all_loads(self) -> List[Dict[str, Any]]:
        """Returns all registered workers with their load scores."""
        items = await self.redis.zrange(REDIS_WORKERS_LOAD_ZSET, 0, -1, withscores=True)
        return [
            {
                "worker_id": (w.decode() if isinstance(w, bytes) else str(w)),
                "load_score": float(score),
            }
            for w, score in items
        ]
