import hashlib
import math
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
from sqlalchemy import select, func, desc
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis

from app.core.config import get_settings
from app.models.job import Job, JobStatus, JobAttempt, AttemptStatus, JobDependency
from app.models.worker import WorkerNode, WorkerStatus
from app.scheduler.load_balancer import compute_worker_load_score
from app.schemas.dashboard import (
    DashboardHubNode,
    DashboardWorkerNode,
    DashboardJobAttempt,
    DashboardJobNode,
    DashboardEdge,
    DashboardStats,
    DashboardEvent,
    DashboardSnapshotResponse,
)


def calculate_percentile(data: List[float], p: float) -> float:
    """Calculates nearest-rank percentile from a sorted float array."""
    if not data:
        return 0.0
    k = (len(data) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return round(data[int(k)], 2)
    return round(data[int(f)] * (c - k) + data[int(c)] * (k - f), 2)


class DashboardService:
    """Aggregates real-time read-only topology snapshot for the visual graph dashboard."""

    @staticmethod
    async def get_snapshot(
        session: AsyncSession,
        redis: Redis,
        if_none_match: Optional[str] = None,
    ) -> Tuple[Optional[DashboardSnapshotResponse], str, bool]:
        """Fetches full visual graph topology and stats.
        
        Returns:
            (snapshot, etag, is_not_modified)
            If if_none_match matches calculated etag, snapshot is None and is_not_modified is True.
        """
        settings = get_settings()
        now = datetime.now(timezone.utc)
        cutoff_dead_dt = now - timedelta(seconds=float(settings.WORKER_DEAD_THRESHOLD))

        # 1. Redis Queue Depths
        priority_depth = await redis.zcard("task_engine:queue:priority")
        delayed_depth = await redis.zcard("task_engine:queue:delayed")
        dlq_depth = await redis.zcard("task_engine:queue:dlq")

        hub_node = DashboardHubNode(
            id="hub",
            label="Central Scheduler",
            queue_depth=priority_depth,
            delayed_depth=delayed_depth,
            dlq_depth=dlq_depth,
        )

        # 2. Worker Nodes from Postgres & Redis
        worker_stmt = select(WorkerNode).order_by(WorkerNode.registered_at.asc())
        worker_res = await session.execute(worker_stmt)
        worker_records = list(worker_res.scalars().all())

        worker_nodes: List[DashboardWorkerNode] = []
        active_workers_count = 0

        for w in worker_records:
            hb_age_sec = max(0.0, (now - w.last_heartbeat_at).total_seconds())
            is_alive = (w.status == WorkerStatus.HEALTHY and w.last_heartbeat_at >= cutoff_dead_dt)
            if is_alive:
                active_workers_count += 1

            load_score = compute_worker_load_score(
                cpu_percent=w.cpu_percent,
                memory_percent=w.memory_percent,
                running_jobs=w.running_jobs_count,
                concurrency=w.concurrency,
            )

            worker_nodes.append(
                DashboardWorkerNode(
                    id=w.id,
                    hostname=w.hostname,
                    pid=w.pid,
                    status=w.status.value,
                    is_alive=is_alive,
                    heartbeat_age_seconds=round(hb_age_sec, 1),
                    concurrency=w.concurrency,
                    running_jobs=w.running_jobs_count,
                    cpu_percent=round(w.cpu_percent, 1),
                    memory_percent=round(w.memory_percent, 1),
                    load_score=load_score,
                )
            )

        # 3. Overall Job Status Counts
        status_counts_stmt = select(Job.status, func.count(Job.id)).group_by(Job.status)
        status_res = await session.execute(status_counts_stmt)
        status_counts: Dict[str, int] = {s.value: 0 for s in JobStatus}
        total_jobs_in_db = 0
        for status_val, count in status_res.all():
            status_str = status_val.value if hasattr(status_val, "value") else str(status_val)
            status_counts[status_str] = count
            total_jobs_in_db += count

        # 4. Fetch Rendered Jobs (Capped at 300: active first, then recent terminal)
        # Active jobs (RUNNING, QUEUED, PENDING)
        active_statuses = [JobStatus.RUNNING, JobStatus.QUEUED, JobStatus.PENDING]
        active_stmt = (
            select(Job)
            .where(Job.status.in_(active_statuses))
            .order_by(Job.priority.desc(), Job.created_at.asc())
            .limit(150)
            .options(
                selectinload(Job.attempts),
                selectinload(Job.dependencies),
            )
        )
        active_res = await session.execute(active_stmt)
        rendered_job_models: List[Job] = list(active_res.scalars().all())

        # Remaining capacity for recent terminal jobs
        remaining_slots = max(0, 300 - len(rendered_job_models))
        if remaining_slots > 0:
            terminal_stmt = (
                select(Job)
                .where(~Job.status.in_(active_statuses))
                .order_by(Job.created_at.desc())
                .limit(remaining_slots)
                .options(
                    selectinload(Job.attempts),
                    selectinload(Job.dependencies),
                )
            )
            term_res = await session.execute(terminal_stmt)
            rendered_job_models.extend(term_res.scalars().all())

        # Index jobs for dependency status resolution
        job_map: Dict[str, Job] = {str(j.id): j for j in rendered_job_models}

        # Query all parent jobs of rendered jobs if not already in memory
        missing_parent_ids = set()
        for j in rendered_job_models:
            for dep in j.dependencies:
                dep_id_str = str(dep.depends_on_job_id)
                if dep_id_str not in job_map:
                    missing_parent_ids.add(dep.depends_on_job_id)

        parent_status_cache: Dict[str, JobStatus] = {
            str(j.id): j.status for j in rendered_job_models
        }
        if missing_parent_ids:
            p_stmt = select(Job.id, Job.status).where(Job.id.in_(list(missing_parent_ids)))
            p_res = await session.execute(p_stmt)
            for p_id, p_st in p_res.all():
                parent_status_cache[str(p_id)] = p_st

        # 5. Build Job Nodes & Graph Edges
        job_nodes: List[DashboardJobNode] = []
        edges: List[DashboardEdge] = []

        terminal_failures = {JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.DEAD}

        for j in rendered_job_models:
            j_id_str = str(j.id)
            dep_id_strings = [str(dep.depends_on_job_id) for dep in j.dependencies]

            # Check if this job is blocked by a failed parent
            is_blocked = False
            if j.status == JobStatus.PENDING:
                for parent_str in dep_id_strings:
                    parent_st = parent_status_cache.get(parent_str)
                    if parent_st in terminal_failures:
                        is_blocked = True
                        break

            # Convert attempts
            attempts_list = [
                DashboardJobAttempt(
                    attempt_number=att.attempt_number,
                    worker_id=att.worker_id,
                    status=att.status.value,
                    started_at=att.started_at,
                    finished_at=att.finished_at,
                    duration_ms=att.execution_duration_ms,
                    error=att.error,
                )
                for att in j.attempts
            ]

            job_nodes.append(
                DashboardJobNode(
                    id=j_id_str,
                    job_type=j.job_type,
                    status=j.status.value,
                    priority=j.priority,
                    worker_id=j.worker_id,
                    created_at=j.created_at,
                    scheduled_at=j.scheduled_at,
                    started_at=j.started_at,
                    finished_at=j.finished_at,
                    retry_count=j.retry_count,
                    max_retries=j.max_retries,
                    timeout_seconds=j.timeout_seconds,
                    idempotency_key=j.idempotency_key,
                    result=j.result,
                    error=j.error,
                    depends_on=dep_id_strings,
                    attempts=attempts_list,
                    is_blocked=is_blocked,
                )
            )

            # Edge 1: Queue Edge (Hub -> Queued Job)
            if j.status == JobStatus.QUEUED:
                edges.append(
                    DashboardEdge(
                        id=f"edge-hub-{j_id_str}",
                        source="hub",
                        target=j_id_str,
                        edge_type="queue",
                        status="active",
                        priority=j.priority,
                    )
                )

            # Edge 2: Assignment Edge (Worker -> Running Job)
            elif j.status == JobStatus.RUNNING and j.worker_id:
                edges.append(
                    DashboardEdge(
                        id=f"edge-w-{j.worker_id}-{j_id_str}",
                        source=j.worker_id,
                        target=j_id_str,
                        edge_type="assignment",
                        status="active",
                        priority=j.priority,
                    )
                )

            # Edge 3: DAG Dependency Edges (Parent Job -> Child Job)
            for parent_str in dep_id_strings:
                parent_st = parent_status_cache.get(parent_str)
                if parent_st == JobStatus.SUCCEEDED:
                    edge_st = "satisfied"
                elif parent_st in terminal_failures:
                    edge_st = "blocked_failed"
                else:
                    edge_st = "waiting"

                edges.append(
                    DashboardEdge(
                        id=f"edge-dep-{parent_str}-{j_id_str}",
                        source=parent_str,
                        target=j_id_str,
                        edge_type="dependency",
                        status=edge_st,
                        priority=j.priority,
                    )
                )

        # 6. Real Latency Calculation from DB (Recent 500 completed attempts)
        latency_stmt = (
            select(JobAttempt.execution_duration_ms)
            .where(JobAttempt.execution_duration_ms.isnot(None))
            .order_by(JobAttempt.started_at.desc())
            .limit(500)
        )
        latency_res = await session.execute(latency_stmt)
        durations = [float(d) for d in latency_res.scalars().all() if d is not None]
        durations.sort()

        p50 = calculate_percentile(durations, 50)
        p95 = calculate_percentile(durations, 95)
        p99 = calculate_percentile(durations, 99)

        # 7. Completed Throughput & Failure Rate
        succeeded_jobs = status_counts.get("SUCCEEDED", 0)
        failed_jobs = status_counts.get("FAILED", 0) + status_counts.get("DEAD", 0)
        total_terminal = succeeded_jobs + failed_jobs
        failure_rate = round((failed_jobs / max(1, total_terminal)) * 100.0, 2) if total_terminal > 0 else 0.0

        # Throughput in last 60 seconds
        recent_cutoff = now - timedelta(seconds=60)
        throughput_stmt = select(func.count(JobAttempt.id)).where(
            JobAttempt.finished_at >= recent_cutoff,
            JobAttempt.status == AttemptStatus.SUCCEEDED,
        )
        tp_res = await session.execute(throughput_stmt)
        completed_in_window = tp_res.scalar_one() or 0
        throughput_jobs_sec = round(completed_in_window / 60.0, 1)

        stats = DashboardStats(
            queue_depth=priority_depth,
            delayed_depth=delayed_depth,
            dlq_depth=dlq_depth,
            active_workers=active_workers_count,
            total_workers=len(worker_records),
            running_jobs=status_counts.get("RUNNING", 0),
            total_jobs=total_jobs_in_db,
            succeeded_jobs=succeeded_jobs,
            failed_jobs=failed_jobs,
            failure_rate_percent=failure_rate,
            throughput_jobs_per_sec=throughput_jobs_sec,
            latency_p50_ms=p50,
            latency_p95_ms=p95,
            latency_p99_ms=p99,
        )

        # 8. Event Feed: Last 50 state transitions / events
        events_stmt = (
            select(JobAttempt, Job.job_type)
            .join(Job, Job.id == JobAttempt.job_id)
            .order_by(JobAttempt.started_at.desc())
            .limit(50)
        )
        events_res = await session.execute(events_stmt)
        event_rows = events_res.all()

        events: List[DashboardEvent] = []
        for att, job_type in event_rows:
            att_job_id = str(att.job_id)
            ts = att.finished_at or att.started_at

            if att.error and "died or missed heartbeats" in att.error:
                ev_type = "WORKER_LOST"
                severity = "error"
                msg = f"Worker [{att.worker_id}] lost. Requeuing job {att_job_id[:8]} ({job_type})"
            elif att.status == AttemptStatus.SUCCEEDED:
                ev_type = "JOB_SUCCEEDED"
                severity = "success"
                dur = f" in {att.execution_duration_ms:.0f}ms" if att.execution_duration_ms else ""
                msg = f"Job {att_job_id[:8]} ({job_type}) completed on [{att.worker_id}]{dur}"
            elif att.status == AttemptStatus.TIMED_OUT:
                ev_type = "JOB_TIMEOUT"
                severity = "warning"
                msg = f"Job {att_job_id[:8]} ({job_type}) timed out on [{att.worker_id}] (attempt #{att.attempt_number})"
            elif att.status == AttemptStatus.FAILED:
                ev_type = "JOB_FAILED"
                severity = "error"
                msg = f"Job {att_job_id[:8]} ({job_type}) failed on [{att.worker_id}]: {att.error or 'error'}"
            else:
                ev_type = "JOB_STARTED"
                severity = "info"
                msg = f"Worker [{att.worker_id}] running job {att_job_id[:8]} ({job_type}) attempt #{att.attempt_number}"

            events.append(
                DashboardEvent(
                    id=f"ev-{att.id}",
                    timestamp=ts,
                    event_type=ev_type,
                    job_id=att_job_id,
                    worker_id=att.worker_id,
                    message=msg,
                    severity=severity,
                )
            )

        # 9. Compute ETag
        latest_attempt_id = str(event_rows[0][0].id) if event_rows else "none"
        raw_etag = (
            f"{priority_depth}:{delayed_depth}:{dlq_depth}:"
            f"{stats.running_jobs}:{stats.total_jobs}:{latest_attempt_id}:"
            f"{active_workers_count}"
        )
        etag = hashlib.sha256(raw_etag.encode()).hexdigest()[:16]

        if if_none_match and if_none_match.strip('\"') == etag:
            return None, etag, True

        snapshot = DashboardSnapshotResponse(
            hub=hub_node,
            workers=worker_nodes,
            jobs=job_nodes,
            edges=edges,
            stats=stats,
            events=events,
            status_counts=status_counts,
            total_rendered_jobs=len(job_nodes),
            total_unrendered_jobs=max(0, total_jobs_in_db - len(job_nodes)),
            etag=etag,
        )

        return snapshot, etag, False
