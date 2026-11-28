import asyncio
from dataclasses import dataclass
import time
from typing import Dict, List, Optional, Tuple
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
    REGISTRY,
)
from prometheus_client.core import CounterMetricFamily, HistogramMetricFamily
from redis.asyncio import Redis
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

# Fixed buckets for execution duration histogram (in seconds)
EXECUTION_DURATION_BUCKETS: Tuple[float, ...] = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0
)
TERMINAL_JOB_STATUSES: Tuple[str, ...] = ("SUCCEEDED", "FAILED", "DEAD", "CANCELLED")

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

# 3. Status distribution gauge
JOBS_BY_STATUS = Gauge(
    "task_engine_jobs_by_status",
    "Current number of jobs in database by status",
    ["status"],
)

# 4. Throughput metrics
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

# 5. Latency histograms (p50 / p95 / p99 precision)
JOB_EXECUTION_DURATION = Histogram(
    "task_engine_job_execution_duration_seconds",
    "Execution duration of tasks in seconds",
    ["job_type"],
    buckets=EXECUTION_DURATION_BUCKETS,
)

JOB_SCHEDULING_LATENCY = Histogram(
    "task_engine_job_scheduling_latency_seconds",
    "Time from job submission/creation until execution starts",
    ["job_type"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

# 6. In-flight jobs
RUNNING_JOBS = Gauge(
    "task_engine_running_jobs",
    "Current number of concurrently executing jobs",
)


@dataclass
class _DbMetricsSnapshot:
    completed_by_status: Dict[str, int]
    status_totals: Dict[str, int]
    running_count: int
    submitted_by_type: Dict[str, int]
    retried_by_type: Dict[str, int]
    duration_buckets: List[Tuple[str, int]]
    duration_sum: float


class PostgresMetricsCollector:
    """Custom Prometheus collector computing monotonic completed job counts and
    execution duration histograms directly from PostgreSQL with a short TTL cache.
    """

    def __init__(self, cache_ttl_seconds: float = 5.0) -> None:
        self.cache_ttl_seconds = cache_ttl_seconds
        self._snapshot: Optional[_DbMetricsSnapshot] = None
        self._last_refresh_ts: float = 0.0
        self._lock = asyncio.Lock()

    def invalidate_cache(self) -> None:
        """Clears the cached snapshot so the next refresh queries PostgreSQL."""
        self._snapshot = None
        self._last_refresh_ts = 0.0

    def _is_cache_valid(self) -> bool:
        return (
            self._snapshot is not None
            and (time.monotonic() - self._last_refresh_ts) < self.cache_ttl_seconds
        )

    async def refresh(self, session: AsyncSession, force: bool = False) -> None:
        """Refreshes cached database metrics if the cache TTL has expired."""
        if not force and self._is_cache_valid():
            return

        async with self._lock:
            if not force and self._is_cache_valid():
                return

            from app.models.job import Job, JobAttempt, JobStatus

            # 1. Job counts by status and job_type
            status_stmt = select(Job.status, Job.job_type, func.count(Job.id)).group_by(
                Job.status, Job.job_type
            )
            status_res = await session.execute(status_stmt)
            status_rows = status_res.all()

            running_count = 0
            status_totals: Dict[str, int] = {st.value: 0 for st in JobStatus}
            completed_by_status: Dict[str, int] = {st: 0 for st in TERMINAL_JOB_STATUSES}
            submitted_by_type: Dict[str, int] = {}

            for st, jtype, count in status_rows:
                status_val = st.value if hasattr(st, "value") else str(st)
                cnt_int = int(count)
                status_totals[status_val] = status_totals.get(status_val, 0) + cnt_int
                submitted_by_type[jtype] = submitted_by_type.get(jtype, 0) + cnt_int

                if status_val == JobStatus.RUNNING.value:
                    running_count += cnt_int
                if status_val in completed_by_status:
                    completed_by_status[status_val] += cnt_int

            # 2. Retry counts by job_type
            retry_stmt = select(
                Job.job_type, func.coalesce(func.sum(Job.retry_count), 0)
            ).group_by(Job.job_type)
            retry_res = await session.execute(retry_stmt)
            retried_by_type: Dict[str, int] = {
                jtype: int(retries) for jtype, retries in retry_res.all()
            }

            # 3. Execution duration histogram from job_attempts started_at/finished_at
            attempts_stmt = select(
                JobAttempt.started_at, JobAttempt.finished_at
            ).where(
                JobAttempt.started_at.isnot(None),
                JobAttempt.finished_at.isnot(None),
            )
            attempts_res = await session.execute(attempts_stmt)

            bucket_counts = [0] * len(EXECUTION_DURATION_BUCKETS)
            inf_count = 0
            duration_sum = 0.0

            for started_at, finished_at in attempts_res.all():
                if started_at is None or finished_at is None:
                    continue
                dur_sec = max(0.0, (finished_at - started_at).total_seconds())
                duration_sum += dur_sec
                inf_count += 1
                for idx, bound in enumerate(EXECUTION_DURATION_BUCKETS):
                    if dur_sec <= bound:
                        bucket_counts[idx] += 1

            duration_buckets: List[Tuple[str, int]] = [
                (str(bound), bucket_counts[idx])
                for idx, bound in enumerate(EXECUTION_DURATION_BUCKETS)
            ] + [("+Inf", inf_count)]

            self._snapshot = _DbMetricsSnapshot(
                completed_by_status=completed_by_status,
                status_totals=status_totals,
                running_count=running_count,
                submitted_by_type=submitted_by_type,
                retried_by_type=retried_by_type,
                duration_buckets=duration_buckets,
                duration_sum=duration_sum,
            )
            self._last_refresh_ts = time.monotonic()

            # Sync auxiliary gauges/counters on REGISTRY
            for st_val, cnt in status_totals.items():
                JOBS_BY_STATUS.labels(status=st_val).set(cnt)
            RUNNING_JOBS.set(running_count)

            for jtype, count in submitted_by_type.items():
                current = JOBS_SUBMITTED.labels(job_type=jtype)._value.get()
                delta = count - current
                if delta > 0:
                    JOBS_SUBMITTED.labels(job_type=jtype).inc(delta)

            for jtype, retries_int in retried_by_type.items():
                current = JOBS_RETRIED.labels(job_type=jtype)._value.get()
                delta = retries_int - current
                if delta > 0:
                    JOBS_RETRIED.labels(job_type=jtype).inc(delta)

    def collect(self):
        """Yields Prometheus metric families from the cached PostgreSQL snapshot."""
        completed_family = CounterMetricFamily(
            "task_engine_db_jobs_completed",
            "Total number of completed jobs by status computed from PostgreSQL",
            labels=["status"],
        )
        duration_family = HistogramMetricFamily(
            "task_engine_db_job_execution_duration_seconds",
            "Execution duration of job attempts in seconds computed from PostgreSQL started_at/finished_at",
            labels=[],
        )

        if self._snapshot is not None:
            for status_val, cnt in self._snapshot.completed_by_status.items():
                completed_family.add_metric([status_val], cnt)
            duration_family.add_metric(
                [],
                buckets=self._snapshot.duration_buckets,
                sum_value=self._snapshot.duration_sum,
            )
        else:
            for status_val in TERMINAL_JOB_STATUSES:
                completed_family.add_metric([status_val], 0)
            empty_buckets = [(str(b), 0) for b in EXECUTION_DURATION_BUCKETS] + [("+Inf", 0)]
            duration_family.add_metric([], buckets=empty_buckets, sum_value=0.0)

        yield completed_family
        yield duration_family


postgres_collector = PostgresMetricsCollector(cache_ttl_seconds=5.0)
REGISTRY.register(postgres_collector)


async def collect_system_metrics(
    redis: Optional[Redis] = None,
    session: Optional[AsyncSession] = None,
) -> bytes:
    """Refreshes live queue depths, worker counts, and cached PostgreSQL telemetry
    before returning Prometheus formatted text.
    """
    # 1. Collect live Redis coordination metrics
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

    # 2. Refresh cached PostgreSQL custom collector snapshot
    if session is not None:
        try:
            await postgres_collector.refresh(session)
        except Exception:
            pass

    return generate_latest(REGISTRY)
