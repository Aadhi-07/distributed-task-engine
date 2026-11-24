#!/usr/bin/env python3
"""
DISTASKENG Live Dashboard Workload Generator

Submits:
  1. A Diamond DAG with a deliberately failing branch:
       root -> [branch_fail (failing_task), branch_ok (model_inference)] -> join_task
       Demonstrates: Cascading failure, dependency blocking, red DAG edges in UI.
  2. A burst of N jobs with mixed priorities (0-9) and diverse workloads.

Usage:
  python scripts/demo.py
  python scripts/demo.py --api-url http://localhost:8000 --burst 30
  python scripts/demo.py --run-worker  # Runs an ephemeral worker for 10s to execute jobs
"""

import argparse
import asyncio
import sys
import uuid
from pathlib import Path
from typing import Dict, Any, List, Optional

# Ensure repository root is on sys.path
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import httpx
from app.core.database import get_session_factory, close_database_connection
from app.core.redis import get_redis, close_redis
from app.schemas.job import JobCreate
from app.services.job_service import JobService
from app.worker.main import WorkerProcess


async def submit_via_http(api_url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Submits a job via HTTP POST to the running API server."""
    async with httpx.AsyncClient(base_url=api_url, timeout=10.0) as client:
        res = await client.post("/api/v1/jobs", json=payload)
        res.raise_for_status()
        return res.json()


async def submit_via_db(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Direct submission via JobService when running without standalone HTTP server."""
    session_factory = get_session_factory()
    redis = await get_redis()
    async with session_factory() as session:
        job_create = JobCreate(**payload)
        job, _ = await JobService.submit_job(session, redis, job_create)
        return {
            "id": str(job.id),
            "job_type": job.job_type,
            "status": job.status.value,
            "priority": job.priority,
        }


async def run_demo(api_url: str, burst_count: int, run_worker: bool):
    print("=" * 70)
    print("🚀 DISTASKENG LIVE WORKLOAD & DAG GENERATOR")
    print(f"   Target: {api_url} | Burst size: {burst_count} jobs | Run worker: {run_worker}")
    print("=" * 70)

    # Check whether the HTTP API is reachable
    use_http = False
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            resp = await client.get(f"{api_url}/health")
            if resp.status_code == 200:
                use_http = True
                print(f"[*] Connected to live API server at {api_url}")
    except Exception:
        print(f"[!] API server at {api_url} not reachable. Submitting directly to DB & Redis.")

    submit_fn = (lambda p: submit_via_http(api_url, p)) if use_http else submit_via_db

    # --------------------------------------------------------------------------
    # 1. Submit Diamond DAG with Deliberately Failing Branch
    # --------------------------------------------------------------------------
    print("\n[Phase 1] Submitting Diamond DAG with Deliberately Failing Branch...")
    print("          root -> [branch_fail, branch_ok] -> join_task")

    # Step 1.1: Root job
    root_job = await submit_fn({
        "job_type": "model_training",
        "payload": {"epochs": 2, "batch_size": 32, "model_name": "bert-small"},
        "priority": 8,
    })
    root_id = root_job["id"]
    print(f"  \u2714 Root Job submitted: {root_id} (Priority: 8, Type: model_training)")

    # Step 1.2: Branch A (Deliberately failing task, max_retries=1)
    branch_fail = await submit_fn({
        "job_type": "failing_task",
        "payload": {"error_message": "Fault Injection: OOM in matrix multiply"},
        "priority": 7,
        "max_retries": 1,
        "depends_on": [root_id],
    })
    fail_id = branch_fail["id"]
    print(f"  \u2714 Branch A (Failing): {fail_id} (depends on root, max_retries: 1)")

    # Step 1.3: Branch B (Healthy task)
    branch_ok = await submit_fn({
        "job_type": "model_inference",
        "payload": {"features": [0.12, 0.45, 0.89], "model_name": "resnet-50"},
        "priority": 6,
        "depends_on": [root_id],
    })
    ok_id = branch_ok["id"]
    print(f"  \u2714 Branch B (Healthy): {ok_id} (depends on root)")

    # Step 1.4: Join task (Depends on both branch_fail and branch_ok)
    join_job = await submit_fn({
        "job_type": "batch_scoring",
        "payload": {"dataset_id": "eval-2026", "metric": "f1_score"},
        "priority": 9,
        "depends_on": [fail_id, ok_id],
    })
    join_id = join_job["id"]
    print(f"  \u2714 Join Task:         {join_id} (depends on {fail_id[:8]} & {ok_id[:8]})")
    print("  -> Expected topology behavior: When Branch A exhausts retries, Join Task is marked FAILED (blocked).")

    # --------------------------------------------------------------------------
    # 2. Submit Burst of N Jobs with Mixed Priorities
    # --------------------------------------------------------------------------
    print(f"\n[Phase 2] Submitting burst of {burst_count} jobs with mixed priorities (0-9)...")

    workload_configs = [
        {"type": "model_inference", "prio": 9, "payload": {"features": [1.0, 2.0, 3.0]}},
        {"type": "model_inference", "prio": 8, "payload": {"features": [0.5, 0.9, 1.2]}},
        {"type": "batch_scoring", "prio": 7, "payload": {"dataset": "prod-v1", "records": 500}},
        {"type": "model_training", "prio": 6, "payload": {"epochs": 3, "lr": 0.001}},
        {"type": "sleep", "prio": 5, "payload": {"seconds": 0.1}},
        {"type": "batch_scoring", "prio": 4, "payload": {"dataset": "prod-v2", "records": 200}},
        {"type": "sleep", "prio": 2, "payload": {"seconds": 0.2}},
        {"type": "model_inference", "prio": 1, "payload": {"features": [0.0, 0.0]}},
        {"type": "sleep", "prio": 0, "payload": {"seconds": 0.05}},
    ]

    submitted_ids = []
    for i in range(burst_count):
        cfg = workload_configs[i % len(workload_configs)]
        job = await submit_fn({
            "job_type": cfg["type"],
            "payload": cfg["payload"],
            "priority": cfg["prio"],
        })
        submitted_ids.append(job["id"])

    print(f"  \u2714 Successfully queued {len(submitted_ids)} burst jobs with priorities 0-9.")

    # --------------------------------------------------------------------------
    # 3. Optional Ephemeral Worker Execution
    # --------------------------------------------------------------------------
    if run_worker:
        print("\n[Phase 3] Starting ephemeral worker 'demo-worker-fleet' for 6 seconds...")
        worker = WorkerProcess(worker_id="demo-worker-fleet", concurrency=4)
        worker_task = asyncio.create_task(worker.start())

        for tick in range(6):
            await asyncio.sleep(1.0)
            print(f"  [*] Worker running... ({tick + 1}/6s)")

        await worker.stop()
        worker_task.cancel()
        print("  \u2714 Ephemeral worker finished execution cycles.")

    print("\n" + "=" * 70)
    print("✨ DEMO WORKLOAD READY")
    print(f"   Open your browser to: {api_url}/dashboard")
    print("   Observe:")
    print("     1. Central Queue Hub with active queue depth counter")
    print("     2. Worker orbital ring displaying real-time load score")
    print("     3. Pulsing cyan edges for running jobs")
    print("     4. Red blocked dependency edges on the Diamond DAG failure")
    print("     5. Real-time event log with state transitions and inspector pane")
    print("=" * 70)

    # Cleanup DB/Redis resources
    await close_database_connection()
    await close_redis()


def main():
    parser = argparse.ArgumentParser(description="DISTASKENG Live Dashboard Workload Generator")
    parser.add_argument("--api-url", default="http://localhost:8000", help="FastAPI base URL (default: http://localhost:8000)")
    parser.add_argument("--burst", type=int, default=30, help="Number of mixed-priority burst jobs to submit (default: 30)")
    parser.add_argument("--run-worker", action="store_true", help="Run an ephemeral worker for 6s to process workloads live")
    args = parser.parse_args()

    asyncio.run(run_demo(args.api_url, args.burst, args.run_worker))


if __name__ == "__main__":
    main()
