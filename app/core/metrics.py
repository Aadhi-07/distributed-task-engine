import time
from typing import Optional, Set
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
    REGISTRY,
)
from redis.asyncio import Redis
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

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
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
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

_observed_attempt_ids: Set[str] = set()


async def collect_system_metrics(
    redis: Optional[Redis] = None,
    session: Optional[AsyncSession] = None,
) -> bytes:
    """Refreshes live queue depths, worker counts, job statuses, and execution telemetry
    from Redis and PostgreSQL before returning Prometheus formatted text.
    """
    global _observed_attempt_ids

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

    # 2. Collect authoritative PostgreSQL cluster state
    if session is not None:
        try:
            from app.models.job import Job, JobAttempt, JobStatus

            # A. Jobs by status, running count, and completed / submitted counters
            status_stmt = select(Job.status, Job.job_type, func.count(Job.id)).group_by(Job.status, Job.job_type)
            status_res = await session.execute(status_stmt)
            status_rows = status_res.all()

            running_count = 0
            status_totals = {st.value: 0 for st in JobStatus}
            submitted_totals = {}

            for st, jtype, count in status_rows:
                status_val = st.value if hasattr(st, "value") else str(st)
                status_totals[status_val] = status_totals.get(status_val, 0) + count
                submitted_totals[jtype] = submitted_totals.get(jtype, 0) + count

                if status_val == JobStatus.RUNNING.value:
                    running_count += count

                # Sync completed counter from terminal statuses
                if status_val in ("SUCCEEDED", "FAILED", "DEAD"):
                    current = JOBS_COMPLETED.labels(job_type=jtype, status=status_val)._value.get()
                    delta = count - current
                    if delta > 0:
                        JOBS_COMPLETED.labels(job_type=jtype, status=status_val).inc(delta)

            # Update status gauges and running jobs gauge
            for st_val, cnt in status_totals.items():
                JOBS_BY_STATUS.labels(status=st_val).set(cnt)
            RUNNING_JOBS.set(running_count)

            # Sync submitted counter
            for jtype, count in submitted_totals.items():
                current = JOBS_SUBMITTED.labels(job_type=jtype)._value.get()
                delta = count - current
                if delta > 0:
                    JOBS_SUBMITTED.labels(job_type=jtype).inc(delta)

            # B. Sync retried counter from retry_count
            retry_stmt = select(Job.job_type, func.coalesce(func.sum(Job.retry_count), 0)).group_by(Job.job_type)
            retry_res = await session.execute(retry_stmt)
            for jtype, retries in retry_res.all():
                retries_int = int(retries)
                current = JOBS_RETRIED.labels(job_type=jtype)._value.get()
                delta = retries_int - current
                if delta > 0:
                    JOBS_RETRIED.labels(job_type=jtype).inc(delta)

            # C. Sync execution duration histogram from newly finished attempts
            latency_stmt = (
                select(Job.job_type, JobAttempt.execution_duration_ms, JobAttempt.id)
                .join(Job, JobAttempt.job_id == Job.id)
                .where(JobAttempt.execution_duration_ms.isnot(None))
                .order_by(JobAttempt.started_at.desc())
                .limit(200)
            )
            lat_res = await session.execute(latency_stmt)
            new_attempts = []
            for jtype, dur_ms, att_id in lat_res.all():
                att_id_str = str(att_id)
                if att_id_str not in _observed_attempt_ids:
                    new_attempts.append((jtype, dur_ms, att_id_str))

            for jtype, dur_ms, att_id_str in reversed(new_attempts):
                _observed_attempt_ids.add(att_id_str)
                JOB_EXECUTION_DURATION.labels(job_type=jtype).observe(dur_ms / 1000.0)

            # Keep cache bounded
            if len(_observed_attempt_ids) > 5000:
                _observed_attempt_ids = set(list(_observed_attempt_ids)[-2500:])

        except Exception:
            pass

    return generate_latest(REGISTRY)
