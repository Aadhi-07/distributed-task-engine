import asyncio
import json
import math
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

# Ensure repository root is on sys.path
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from httpx import AsyncClient, ASGITransport
from sqlalchemy import select, func

from app.main import app
from app.core.database import get_session_factory, close_database_connection
from app.core.redis import get_redis, close_redis
from app.models.job import Job, JobStatus, JobAttempt
from app.worker.main import WorkerProcess


def percentile(data: List[float], p: float) -> float:
    """Calculates percentile from a sorted float list."""
    if not data:
        return 0.0
    k = (len(data) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return round(data[int(k)], 2)
    d0 = data[int(f)] * (c - k)
    d1 = data[int(c)] * (k - f)
    return round(d0 + d1, 2)


async def run_benchmark(total_jobs: int = 1000, client_concurrency: int = 50, worker_count: int = 6):
    """Executes a high-throughput load benchmark submitting 1000+ jobs and measures real metrics."""
    print("=" * 70)
    print(f"🚀 TASK EXECUTION ENGINE LOAD BENCHMARK: {total_jobs} CONCURRENT JOBS")
    print(f"   Clients: {client_concurrency} concurrent | Workers: {worker_count} processes (4 slots each)")
    print("=" * 70)

    session_factory = get_session_factory()
    redis = await get_redis()
    await redis.flushdb()

    # 1. Start worker pool
    workers: List[WorkerProcess] = [
        WorkerProcess(worker_id=f"bench-worker-{i+1}", concurrency=4)
        for i in range(worker_count)
    ]
    worker_tasks = [asyncio.create_task(w.start()) for w in workers]
    await asyncio.sleep(0.5)  # Allow workers to initialize

    # 2. Prepare 1000+ jobs with mixed priorities and workload types
    job_payloads = []
    job_types = ["model_inference", "batch_scoring", "sleep", "model_training"]
    type_weights = [0.55, 0.25, 0.15, 0.05]

    for i in range(total_jobs):
        selected_type = random.choices(job_types, weights=type_weights)[0]
        prio = random.randint(0, 9)

        if selected_type == "model_inference":
            payload = {"features": [random.random() for _ in range(4)]}
        elif selected_type == "batch_scoring":
            payload = {"batch_size": random.randint(5, 15)}
        elif selected_type == "sleep":
            payload = {"duration": 0.005}
        else:  # model_training
            payload = {"epochs": 1, "learning_rate": 0.01}

        job_payloads.append({
            "job_type": selected_type,
            "payload": payload,
            "priority": prio,
            "timeout_seconds": 60,
            "max_retries": 1,
        })

    # 3. Fire concurrent submissions via ASGI transport
    transport = ASGITransport(app=app)
    submission_semaphore = asyncio.Semaphore(client_concurrency)
    submitted_job_ids: List[uuid.UUID] = []
    submission_latencies_ms: List[float] = []

    submit_start_time = time.monotonic()

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        async def submit_single(payload: Dict[str, Any]):
            async with submission_semaphore:
                t0 = time.monotonic()
                res = await client.post("/jobs", json=payload)
                t1 = time.monotonic()
                submission_latencies_ms.append((t1 - t0) * 1000)
                if res.status_code == 201:
                    submitted_job_ids.append(uuid.UUID(res.json()["id"]))
                else:
                    print(f"Submission failed: {res.status_code} - {res.text}")

        await asyncio.gather(*(submit_single(p) for p in job_payloads))

    submit_total_sec = time.monotonic() - submit_start_time
    submission_throughput = len(submitted_job_ids) / submit_total_sec

    print(f"✅ Submitted {len(submitted_job_ids)}/{total_jobs} jobs in {submit_total_sec:.2f}s "
          f"({submission_throughput:.1f} submissions/sec)")
    print("⏳ Awaiting execution completion across worker pool...")

    # 4. Wait for all submitted jobs to finish processing
    execution_start_time = time.monotonic()
    last_completed = 0
    poll_intervals = 0

    while True:
        await asyncio.sleep(0.5)
        poll_intervals += 1
        async with session_factory() as session:
            stmt = select(func.count(Job.id)).where(
                Job.id.in_(submitted_job_ids),
                Job.status.in_([JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.DEAD, JobStatus.CANCELLED])
            )
            res = await session.execute(stmt)
            completed_count = res.scalar_one()

            if completed_count != last_completed or poll_intervals % 4 == 0:
                print(f"   Progress: {completed_count}/{len(submitted_job_ids)} jobs completed "
                      f"({(completed_count / len(submitted_job_ids) * 100):.1f}%)")
                last_completed = completed_count

            if completed_count >= len(submitted_job_ids):
                break

    execution_total_sec = time.monotonic() - execution_start_time
    completion_throughput = len(submitted_job_ids) / execution_total_sec

    print(f"🎉 All {len(submitted_job_ids)} jobs completed in {execution_total_sec:.2f}s "
          f"({completion_throughput:.1f} jobs/sec)")

    # 5. Stop workers gracefully
    for w in workers:
        w.stop()
    await asyncio.gather(*worker_tasks, return_exceptions=True)

    # 6. Extract precise real metrics from PostgreSQL
    async with session_factory() as session:
        # Load all jobs and attempts
        stmt = select(Job).where(Job.id.in_(submitted_job_ids))
        res = await session.execute(stmt)
        jobs = list(res.scalars().all())

        attempt_stmt = select(JobAttempt).where(JobAttempt.job_id.in_(submitted_job_ids))
        att_res = await session.execute(attempt_stmt)
        attempts = list(att_res.scalars().all())

    # Calculate real scheduling latencies (started_at - created_at)
    scheduling_latencies_ms: List[float] = []
    execution_durations_ms: List[float] = []
    status_counts: Dict[str, int] = {}

    for j in jobs:
        status_str = j.status.value
        status_counts[status_str] = status_counts.get(status_str, 0) + 1

        if j.started_at and j.created_at:
            sched_ms = (j.started_at - j.created_at).total_seconds() * 1000
            scheduling_latencies_ms.append(max(0.0, sched_ms))

    for att in attempts:
        if att.execution_duration_ms is not None:
            execution_durations_ms.append(att.execution_duration_ms)

    submission_latencies_ms.sort()
    scheduling_latencies_ms.sort()
    execution_durations_ms.sort()

    succeeded_count = status_counts.get("SUCCEEDED", 0)
    failed_count = total_jobs - succeeded_count
    failure_rate = (failed_count / total_jobs) * 100.0

    # Build results dictionary with verified real metrics
    results = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_jobs": total_jobs,
        "successful_jobs": succeeded_count,
        "failed_jobs": failed_count,
        "failure_rate_percent": round(failure_rate, 2),
        "workers_count": worker_count,
        "concurrency_per_worker": 4,
        "total_worker_slots": worker_count * 4,
        "submission_duration_sec": round(submit_total_sec, 2),
        "submission_throughput_rps": round(submission_throughput, 1),
        "execution_duration_sec": round(execution_total_sec, 2),
        "processing_throughput_jobs_per_sec": round(completion_throughput, 1),
        "submission_latency_ms": {
            "min": round(submission_latencies_ms[0], 2) if submission_latencies_ms else 0,
            "p50": percentile(submission_latencies_ms, 50),
            "p90": percentile(submission_latencies_ms, 90),
            "p95": percentile(submission_latencies_ms, 95),
            "p99": percentile(submission_latencies_ms, 99),
            "max": round(submission_latencies_ms[-1], 2) if submission_latencies_ms else 0,
        },
        "scheduling_latency_ms": {
            "min": round(scheduling_latencies_ms[0], 2) if scheduling_latencies_ms else 0,
            "p50": percentile(scheduling_latencies_ms, 50),
            "p90": percentile(scheduling_latencies_ms, 90),
            "p95": percentile(scheduling_latencies_ms, 95),
            "p99": percentile(scheduling_latencies_ms, 99),
            "max": round(scheduling_latencies_ms[-1], 2) if scheduling_latencies_ms else 0,
        },
        "execution_duration_ms": {
            "min": round(execution_durations_ms[0], 2) if execution_durations_ms else 0,
            "p50": percentile(execution_durations_ms, 50),
            "p90": percentile(execution_durations_ms, 90),
            "p95": percentile(execution_durations_ms, 95),
            "p99": percentile(execution_durations_ms, 99),
            "max": round(execution_durations_ms[-1], 2) if execution_durations_ms else 0,
        },
        "status_breakdown": status_counts,
    }

    # Save to benchmarks/results.json
    results_path = Path(__file__).resolve().parent / "results.json"
    results_path.write_text(json.dumps(results, indent=2))
    print(f"\n📊 Results written to: {results_path}")

    # Display clean table
    print("\n" + "=" * 70)
    print("📈 LOAD BENCHMARK FINAL METRICS REPORT")
    print("=" * 70)
    print(f"Total Jobs Submitted:      {results['total_jobs']}")
    print(f"Total Jobs Succeeded:      {results['successful_jobs']} ({100 - results['failure_rate_percent']:.1f}%)")
    print(f"Total Jobs Failed:         {results['failed_jobs']} ({results['failure_rate_percent']:.2f}%)")
    print(f"Submission Throughput:     {results['submission_throughput_rps']} req/s")
    print(f"Processing Throughput:     {results['processing_throughput_jobs_per_sec']} jobs/s")
    print("-" * 70)
    print(f"Scheduling Latency (p50):  {results['scheduling_latency_ms']['p50']} ms")
    print(f"Scheduling Latency (p95):  {results['scheduling_latency_ms']['p95']} ms")
    print(f"Scheduling Latency (p99):  {results['scheduling_latency_ms']['p99']} ms")
    print(f"Scheduling Latency (max):  {results['scheduling_latency_ms']['max']} ms")
    print("-" * 70)
    print(f"Execution Duration (p50):  {results['execution_duration_ms']['p50']} ms")
    print(f"Execution Duration (p95):  {results['execution_duration_ms']['p95']} ms")
    print(f"Execution Duration (p99):  {results['execution_duration_ms']['p99']} ms")
    print("=" * 70)

    await close_database_connection()
    await close_redis()
    return results


if __name__ == "__main__":
    asyncio.run(run_benchmark(total_jobs=1000, client_concurrency=50, worker_count=6))
