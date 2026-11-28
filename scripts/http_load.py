#!/usr/bin/env python3
"""
HTTP Load Generator for Distributed Task Execution Engine

Submits mixed-priority jobs to a deployed or local API over network HTTP (using httpx),
polls until all jobs reach terminal states, and computes real metrics:
throughput, p50/p95/p99 execution latency, and failure rate.

Usage:
  python scripts/http_load.py --base-url https://task-engine-api.onrender.com --api-key my-key --jobs 50 --concurrency 10
  python scripts/http_load.py --base-url http://localhost:8000 --jobs 30 --concurrency 5
"""

import argparse
import asyncio
from datetime import datetime
import math
import sys
import time
from typing import Any, Dict, List, Optional
import httpx


def calculate_percentile(sorted_values: List[float], p: float) -> Optional[float]:
    """Calculates percentile from a sorted float array, or None if empty."""
    if not sorted_values:
        return None
    k = (len(sorted_values) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return round(sorted_values[int(k)], 2)
    d0 = sorted_values[int(f)] * (c - k)
    d1 = sorted_values[int(c)] * (k - f)
    return round(d0 + d1, 2)


def extract_execution_duration_ms(info: Dict[str, Any]) -> Optional[float]:
    """Extracts real execution duration in ms from job timestamps or attempts, or None if missing."""
    started_raw = info.get("started_at")
    finished_raw = info.get("finished_at")
    if started_raw and finished_raw:
        try:
            started_dt = datetime.fromisoformat(started_raw.replace("Z", "+00:00"))
            finished_dt = datetime.fromisoformat(finished_raw.replace("Z", "+00:00"))
            return max(0.0, (finished_dt - started_dt).total_seconds() * 1000.0)
        except ValueError:
            pass

    attempts = info.get("attempts") or []
    if attempts:
        last_att = attempts[-1]
        att_dur = last_att.get("execution_duration_ms")
        if att_dur is not None:
            return float(att_dur)
        att_start = last_att.get("started_at")
        att_finish = last_att.get("finished_at")
        if att_start and att_finish:
            try:
                s_dt = datetime.fromisoformat(att_start.replace("Z", "+00:00"))
                f_dt = datetime.fromisoformat(att_finish.replace("Z", "+00:00"))
                return max(0.0, (f_dt - s_dt).total_seconds() * 1000.0)
            except ValueError:
                pass

    duration = info.get("execution_duration_ms")
    if duration is not None:
        return float(duration)
    return None


WORKLOAD_TEMPLATES = [
    {"job_type": "model_inference", "priority": 9, "payload": {"features": [1.2, 3.4, 5.6], "model": "resnet"}},
    {"job_type": "batch_scoring", "priority": 7, "payload": {"dataset": "prod-v1", "batch_size": 256}},
    {"job_type": "model_training", "priority": 5, "payload": {"epochs": 2, "learning_rate": 0.01}},
    {"job_type": "sleep", "priority": 3, "payload": {"seconds": 0.05}},
    {"job_type": "model_inference", "priority": 1, "payload": {"features": [0.1, 0.2]}},
    {"job_type": "sleep", "priority": 0, "payload": {"seconds": 0.02}},
]


async def run_load_test(
    base_url: str,
    api_key: Optional[str],
    total_jobs: int,
    concurrency: int,
    poll_interval: float = 1.0,
):
    print("=" * 70)
    print("🚀 DISTASKENG PRODUCTION HTTP LOAD GENERATOR")
    print(f"   Target URL:  {base_url}")
    print(f"   Total Jobs:  {total_jobs}")
    print(f"   Concurrency: {concurrency}")
    print(f"   API Key:     {'[SET]' if api_key else '[NONE]'}")
    print("=" * 70)

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-API-Key"] = api_key

    # Validate endpoint health
    async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as client:
        try:
            health_resp = await client.get("/health/live")
            if health_resp.status_code != 200:
                print(f"[!] Target API health check failed with status {health_resp.status_code}")
                sys.exit(1)
            print("[*] Target API /health/live responded 200 OK. Beginning benchmark...")
        except Exception as exc:
            print(f"[!] Unable to reach target API at {base_url}: {exc}")
            sys.exit(1)

    submission_semaphore = asyncio.Semaphore(concurrency)
    submitted_jobs: List[Dict[str, Any]] = []
    submission_latencies: List[float] = []

    # 1. Job Submission Phase
    start_submission_ts = time.monotonic()

    async def submit_single(client: httpx.AsyncClient, idx: int):
        template = WORKLOAD_TEMPLATES[idx % len(WORKLOAD_TEMPLATES)]
        payload = {
            "job_type": template["job_type"],
            "priority": template["priority"],
            "payload": template["payload"],
        }
        t0 = time.monotonic()
        async with submission_semaphore:
            resp = await client.post("/api/v1/jobs", json=payload)
            elapsed_ms = (time.monotonic() - t0) * 1000.0
            submission_latencies.append(elapsed_ms)
            if resp.status_code in (200, 201):
                data = resp.json()
                submitted_jobs.append({
                    "id": data["id"],
                    "job_type": template["job_type"],
                    "priority": template["priority"],
                    "submitted_at": time.monotonic(),
                })
            else:
                print(f"  [!] Job #{idx+1} submission failed: HTTP {resp.status_code} - {resp.text}")

    async with httpx.AsyncClient(base_url=base_url, headers=headers, timeout=30.0) as client:
        tasks = [asyncio.create_task(submit_single(client, i)) for i in range(total_jobs)]
        await asyncio.gather(*tasks)

    submission_elapsed = time.monotonic() - start_submission_ts
    submission_rate = len(submitted_jobs) / max(submission_elapsed, 0.001)

    print(f"\n[*] Submission complete: {len(submitted_jobs)}/{total_jobs} accepted in {submission_elapsed:.2f}s ({submission_rate:.1f} jobs/s)")

    if not submitted_jobs:
        print("[!] No jobs were successfully submitted. Aborting polling.")
        sys.exit(1)

    # 2. Polling Phase
    print(f"[*] Polling job statuses until terminal states are reached (interval: {poll_interval}s)...")
    pending_job_ids = {j["id"] for j in submitted_jobs}
    completed_jobs: Dict[str, Dict[str, Any]] = {}
    poll_semaphore = asyncio.Semaphore(concurrency)

    async def check_job(client: httpx.AsyncClient, job_id: str):
        async with poll_semaphore:
            try:
                r = await client.get(f"/api/v1/jobs/{job_id}")
                if r.status_code == 200:
                    info = r.json()
                    status = info.get("status")
                    if status in ("SUCCEEDED", "FAILED", "DEAD", "CANCELLED"):
                        return job_id, info
            except Exception:
                pass
        return job_id, None

    overall_start_ts = start_submission_ts
    while pending_job_ids:
        async with httpx.AsyncClient(base_url=base_url, headers=headers, timeout=15.0) as client:
            poll_tasks = [asyncio.create_task(check_job(client, j_id)) for j_id in list(pending_job_ids)]
            results = await asyncio.gather(*poll_tasks)

            for j_id, info in results:
                if info:
                    completed_jobs[j_id] = info
                    pending_job_ids.discard(j_id)

        done_count = len(completed_jobs)
        print(f"  [PROGRESS] {done_count}/{total_jobs} jobs finished ({len(pending_job_ids)} remaining)...", end="\r")
        if pending_job_ids:
            await asyncio.sleep(poll_interval)

    total_execution_time = time.monotonic() - overall_start_ts
    print(f"\n[*] All {len(completed_jobs)} jobs reached terminal state in {total_execution_time:.2f}s.")

    # 3. Metrics Compilation
    status_counts: Dict[str, int] = {}
    execution_durations_ms: List[float] = []

    for job_id, info in completed_jobs.items():
        st = info.get("status", "UNKNOWN")
        status_counts[st] = status_counts.get(st, 0) + 1

        duration = extract_execution_duration_ms(info)
        if duration is not None:
            execution_durations_ms.append(duration)

    execution_durations_ms.sort()
    failed_count = status_counts.get("FAILED", 0) + status_counts.get("DEAD", 0)
    failure_rate = (failed_count / total_jobs) * 100.0
    overall_throughput = total_jobs / max(total_execution_time, 0.001)

    p50_ms = calculate_percentile(execution_durations_ms, 50)
    p95_ms = calculate_percentile(execution_durations_ms, 95)
    p99_ms = calculate_percentile(execution_durations_ms, 99)

    p50_str = f"{p50_ms:.1f} ms" if p50_ms is not None else "n/a"
    p95_str = f"{p95_ms:.1f} ms" if p95_ms is not None else "n/a"
    p99_str = f"{p99_ms:.1f} ms" if p99_ms is not None else "n/a"

    # 4. Display Results
    print("\n" + "=" * 70)
    print("📊 BENCHMARK EXECUTION REPORT (REAL MEASURED DATA)")
    print("=" * 70)
    print(f"Total Workloads:         {total_jobs}")
    print(f"Successful (SUCCEEDED):  {status_counts.get('SUCCEEDED', 0)}")
    print(f"Failed (FAILED / DEAD):  {failed_count}")
    print(f"Other Statuses:          {status_counts}")
    print("-" * 70)
    print(f"Submission Throughput:   {submission_rate:.1f} jobs/s")
    print(f"End-to-End Throughput:   {overall_throughput:.1f} jobs/s")
    print(f"Failure Rate:            {failure_rate:.1f}%")
    print("-" * 70)
    print("Execution Latency Percentiles (Worker Task Duration):")
    print(f"  \u2022 p50 Latency:           {p50_str}")
    print(f"  \u2022 p95 Latency:           {p95_str}")
    print(f"  \u2022 p99 Latency:           {p99_str}")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="DISTASKENG External HTTP Load Generator")
    parser.add_argument("--base-url", default="http://localhost:8000", help="Base URL of deployed API (e.g. https://task-engine-api.onrender.com)")
    parser.add_argument("--api-key", default=None, help="X-API-Key authentication header value if configured")
    parser.add_argument("--jobs", type=int, default=30, help="Total number of jobs to submit (default: 30)")
    parser.add_argument("--concurrency", type=int, default=5, help="Client concurrent connection limit (default: 5)")
    parser.add_argument("--poll-interval", type=float, default=1.0, help="Status polling interval in seconds (default: 1.0)")
    args = parser.parse_args()

    asyncio.run(
        run_load_test(
            base_url=args.base_url.rstrip("/"),
            api_key=args.api_key,
            total_jobs=args.jobs,
            concurrency=args.concurrency,
            poll_interval=args.poll_interval,
        )
    )


if __name__ == "__main__":
    main()
