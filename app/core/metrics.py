import time
from typing import Optional
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
    REGISTRY,
)
from redis.asyncio import Redis

# 1. Queue depth metrics
QUEUE_DEPTH = Gauge(
    "task_engine_queue_depth",
    "Current depth of task engine queues",
    ["queue"],
)

# 2. Worker nodes count
ACTIVE_WORKERS = Gauge(
    "task_engine_active_workers_count",
    "Number of active healthy workers heartbeating",
)

# 3. Throughput metrics
JOBS_SUBMITTED = Counter(
    "task_engine_jobs_submitted_total",
    "Total number of submitted jobs",
    ["job_type"],
)

JOBS_COMPLETED = Counter(
    "task_engine_jobs_completed_total",
    "Total number of completed jobs by status",
    ["job_type", "status"],
)

JOBS_RETRIED = Counter(
    "task_engine_jobs_retried_total",
    "Total number of job retry attempts scheduled",
    ["job_type"],
)

# 4. Latency histograms (p50 / p95 / p99 precision)
JOB_EXECUTION_DURATION = Histogram(
    "task_engine_job_execution_duration_seconds",
    "Execution duration of tasks in seconds",
    ["job_type"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)

JOB_SCHEDULING_LATENCY = Histogram(
    "task_engine_job_scheduling_latency_seconds",
    "Time from job submission/creation until execution starts",
    ["job_type"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

# 5. In-flight jobs
RUNNING_JOBS = Gauge(
    "task_engine_running_jobs",
    "Current number of concurrently executing jobs",
)


async def collect_system_metrics(redis: Optional[Redis] = None) -> bytes:
    """Refreshes live queue depths and active worker counts before returning Prometheus text."""
    if redis is not None:
        try:
            priority_depth = await redis.zcard("task_engine:queue:priority")
            delayed_depth = await redis.zcard("task_engine:queue:delayed")
            dlq_depth = await redis.zcard("task_engine:queue:dlq")

            QUEUE_DEPTH.labels(queue="priority").set(priority_depth)
            QUEUE_DEPTH.labels(queue="delayed").set(delayed_depth)
            QUEUE_DEPTH.labels(queue="dlq").set(dlq_depth)

            now = time.time()
            active_workers = await redis.zcount("task_engine:workers:heartbeats", now - 15, "+inf")
            ACTIVE_WORKERS.set(active_workers)
        except Exception:
            pass

    return generate_latest(REGISTRY)
