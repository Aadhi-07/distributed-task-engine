# Distributed Task Execution Engine - Design Document

## 1. Overview & Architecture

The Distributed Task Execution Engine is a high-performance, fault-tolerant platform for submitting and orchestrating long-running, asynchronous jobs (e.g., machine learning training, inference batching, data pipelines).

### High-Level Topology
```text
  [ Client / SDK ]
         │
         ▼
 ┌──────────────┐       ┌─────────────────┐
 │  FastAPI API │ ◄───► │  PostgreSQL 16  │ (Persistent Jobs, Attempts, Dependencies, Workers)
 └───────┬──────┘       └─────────────────┘
         │
         ▼
 ┌──────────────┐
 │ Redis 7 ZSET │ (Priority Scheduling, Delayed Backoff, Worker Heartbeats)
 └───────┬──────┘
         │
         ▼
 ┌──────────────┐
 │ Worker Pool  │ (Distributed worker daemons, atomic dequeue, heartbeats)
 └──────────────┘
```

## 2. Phase 1 Design Decisions

### 2.1 Persistence Layer: PostgreSQL with SQLAlchemy 2.0 & Alembic
- **SQLAlchemy 2.0 Declarative Mapping**: Strict typing with `Mapped[...]` and `mapped_column(...)`.
- **Async Engine & Connection Pooling**: Utilizes `asyncpg` for non-blocking I/O in the FastAPI event loop, configured with `pool_pre_ping=True` to immediately catch stale database connections.
- **Relational Integrity**:
  - `jobs`: Stores overall job definition, parameters, state machine state, priority, and summary results.
  - `job_attempts`: Append-only execution record tracking every attempt, worker node, stdout/stderr execution logs, and runtime durations.
  - `job_dependencies`: Models DAG edges between jobs (`job_id` depends on `depends_on_job_id`) with unique pair constraints.
  - `workers`: Tracks heartbeat timestamps, hardware load (CPU/memory), concurrency quotas, and lifecycle status.

### 2.2 Coordination Layer: Redis
- **Redis Connection Management**: Shared asynchronous connection pool via `redis.asyncio` with explicit lifecycle hooks.
- **Health Verification**: Active ping probes measuring sub-millisecond latencies to detect network partitions or Redis failovers.

### 2.3 Health & Observability Model
- `GET /health`: Ultra-lightweight endpoint for load balancer pinging.
- `GET /health/live`: Container liveness verification.
- `GET /health/ready`: Deep dependency readiness probe verifying both PostgreSQL and Redis connectivity and recording individual component response latencies. Returns HTTP 503 if any required subsystem is unreachable.

### 2.4 Containerization & Multi-Stage Builds
- Multi-stage Docker builds based on `python:3.12-slim` to eliminate build dependencies and keep image sizes minimal.
- Non-root `appuser` (UID 10001) for strict least-privilege security.
- Explicit Docker Compose dependency ordering using container health checks (`condition: service_healthy` and `condition: service_completed_successfully` for database migrations).

## 3. Phase 2 Design Decisions: Job Submission, Queueing & Single Worker Execution

### 3.1 Task Registry Pattern
- Decoupled task execution using a decorator registry (`@register_task("task_name")`).
- Workloads simulate realistic compute phases:
  - `model_training`: Simulates epochs, learning rate decay, loss tracking, and artifact persistence.
  - `model_inference`: Simulates feature vector evaluation and classification confidence scoring.
  - `batch_scoring`: Simulates partition dataset batch scoring.
  - `sleep`: Configurable async sleep for timeout & latency simulation.
  - `failing_task`: Injects deterministic exceptions for failure recovery and attempt validation.

### 3.2 State Machine & Idempotency
- **Lifecycle Transitions**: `PENDING` -> `QUEUED` -> `RUNNING` -> `SUCCEEDED` / `FAILED`.
- **Idempotency Guarantee**: Submissions with an `idempotency_key` query existing records using a unique database index before creation. If found, the existing job is returned with HTTP 200 without enqueuing duplicates.
- **Race Condition Prevention**: The worker claims jobs using `SELECT ... FOR UPDATE` row-level locks, verifying that the job status is still `QUEUED` before transitioning to `RUNNING` and recording an attempt.

### 3.3 Execution History (JobAttempt)
- Every execution run creates an immutable `JobAttempt` record linking to the parent `Job`.
- Tracks `attempt_number`, `worker_id`, `started_at`, `finished_at`, `status`, `error`, stack traces, and exact `execution_duration_ms`.

## 4. Phase 3 Design Decisions: Priority Queue, Atomic Claims & Timeouts

### 4.1 Priority Scheduling Formula (0-9 with Strict FIFO Tie-Breaking)
Redis Sorted Sets (`ZSET`) order elements by a 64-bit IEEE double-precision float score.
We compute scores using the formula:
$$\text{Score} = (9 - \text{priority}) \times 10^9 + (\text{epoch\_seconds} \bmod 10^9)$$

- **Priority Dominance**: Priority 9 yields an offset of $0 \times 10^9$; Priority 8 yields $1 \times 10^9$, down to Priority 0 yielding $9 \times 10^9$. Because $10^9$ is larger than any modulo time offset, a higher priority job is mathematically guaranteed to have a lower score than any lower priority job.
- **Strict FIFO within Same Priority**: For two jobs with identical priority, the earlier job has an earlier timestamp, producing a smaller score. With `ZPOPMIN`, the older job is popped first.

### 4.2 Atomic Dequeue with Redis Lua Scripting
To eliminate double-execution race conditions when multiple worker processes compete for jobs:
```lua
local item = redis.call('ZPOPMIN', KEYS[1], 1)
if not item or #item == 0 then
    return nil
end
local job_id = item[1]
local score = item[2]
redis.call('HSET', KEYS[2], job_id, ARGV[1] .. ':' .. ARGV[2])
return job_id
```
- Executed atomically on the Redis single thread.
- Pops the candidate job and immediately records it into `task_engine:processing` with the `worker_id` and start timestamp in a single atomic transaction.
- When execution finishes (success, failure, or timeout), `HDEL` removes the job from the in-flight hash.

### 4.3 Per-Job Execution Timeouts
- Uses `asyncio.wait_for(task, timeout=job.timeout_seconds)` to guard execution.
- If a task exceeds its allocated duration:
  - Worker interrupts task execution.
  - Job transitions to `FAILED` with descriptive timeout error message.
  - Attempt records status `TIMED_OUT` with exact duration in milliseconds.
  - The in-flight Redis entry is cleaned up in the `finally` block.

## 5. Phase 4 Design Decisions: Retries, Backoff, DLQ & Cancellation

### 5.1 Exponential Backoff with Jitter
When a task raises an unhandled exception or times out:
- If `retry_count < max_retries`:
  - Computes backoff delay using decorrelated equal jitter:
    $$\text{raw\_delay} = \min(\text{max\_seconds}, \text{base\_seconds} \times 2^{\text{retry\_count}})$$
    $$\text{delay} = \frac{\text{raw\_delay}}{2} + \text{Uniform}\left(0, \frac{\text{raw\_delay}}{2}\right)$$
  - Prevents the classic "thundering herd" problem where multiple simultaneously failing tasks retry simultaneously and hammer downstream dependencies.
  - Increments `retry_count += 1`, persists status as `QUEUED`, and sets `scheduled_at = now + delay`.
  - Pushes into Redis delayed sorted set `task_engine:queue:delayed` with score = `time.time() + delay`.

### 5.2 Atomic Delayed Promotion
- Periodically, or right before claiming jobs, workers execute an atomic Lua script:
  - Queries `ZRANGEBYSCORE task_engine:queue:delayed -inf now LIMIT 0 batch_size`.
  - Atomically pops due items and pushes them into `task_engine:queue:priority` with their recalculated priority score.
  - Ensures seamless resumption of delayed retries without secondary daemon dependencies.

### 5.3 Dead-Letter Queue (DLQ)
- Once a job exhausts all permitted attempts (`retry_count >= max_retries`):
  - Job status transitions to `DEAD`.
  - `finished_at` timestamp is finalized.
  - Job ID is registered in `task_engine:queue:dlq`.
  - The attempt history remains fully preserved for post-mortem analysis.

### 5.4 Job Cancellation
- `POST /jobs/{id}/cancel` endpoint:
  - Checks if job has already reached a terminal state (`SUCCEEDED`, `FAILED`, `CANCELLED`, `DEAD`). If so, returns HTTP 409 Conflict.
  - If `QUEUED`: atomically removes the job from `task_engine:queue:priority` and transitions DB status to `CANCELLED`.
  - If `RUNNING`: sets a Redis cancellation flag (`task_engine:cancel:<id>`) and marks status as `CANCELLED`. Worker detects this flag upon task completion or during execution loops and drops execution gracefully.

## 6. Phase 5 Design Decisions: Heartbeats, Dead Worker Detection & At-Least-Once Delivery

### 6.1 Dual-Tier Heartbeat Tracking
Workers emit heartbeats every `WORKER_HEARTBEAT_INTERVAL` seconds (default 5s):
1. **Redis ZSET (`task_engine:workers:heartbeats`)**:
   - Member: `worker_id`, Score: `epoch_seconds`. Enables $O(\log N)$ range queries for expired worker nodes.
2. **PostgreSQL (`WorkerNode`)**:
   - Tracks `last_heartbeat_at`, `cpu_percent`, `memory_percent`, `running_jobs_count`, and `status`.

### 6.2 Dead Worker Detection & Requeue Algorithm
A background detector runs periodically to evaluate nodes against `WORKER_DEAD_THRESHOLD` (15s):
1. **Node Transition**: Workers with `now - last_heartbeat > threshold` are marked `DEAD`.
2. **Stranded In-Flight Job Recovery**:
   - For every job in status `RUNNING` assigned to the dead worker:
     - Transactionally claims the job via `SELECT ... FOR UPDATE`.
     - Logs a failed attempt in `job_attempts` documenting the crash.
     - **At-Least-Once Guarantee**: If `retry_count < max_retries`, increments `retry_count`, resets status to `QUEUED`, clears `worker_id`, and pushes back into the Redis priority queue.
     - If retries exhausted: transitions status to `DEAD` and sends to `task_engine:queue:dlq`.
     - Clears the in-flight Redis entry `task_engine:processing`.
3. **Idempotency Safeguard**:
   - Jobs maintain unique `idempotency_key` constraints. Even when a dead worker's in-flight task is requeued and picked up by a new worker, the idempotency key prevents duplicate side-effects.

## 7. Phase 6 Design Decisions: Load-Aware Assignment & Concurrency Control

### 7.1 Composite Worker Load Score
Workers report active system metrics during heartbeat pings:
- `running_jobs_count`, `concurrency` (max capacity), `cpu_percent`, and `memory_percent`.
We calculate a normalized composite load score ($0.0$ to $100.0+$):
$$\text{Load Score} = 0.5 \times \left(\frac{\text{running\_jobs}}{\text{concurrency}} \times 100\right) + 0.3 \times \text{cpu\_percent} + 0.2 \times \text{memory\_percent}$$

- Heaviest weight ($50\%$) is given to job slot saturation (`running / concurrency`), directly reflecting worker capacity.
- Secondary weights ($30\%$ CPU, $20\%$ RAM) account for system saturation to avoid overloading CPU- or memory-intensive jobs on already burdened physical nodes.
- Scores are published to a Redis ZSET `task_engine:workers:load` keyed by `worker_id`.

### 7.2 Least-Loaded Worker Targeting & Private Worker Queues
- When assigning jobs, the scheduler queries `ZRANGEBYSCORE task_engine:workers:load -inf +inf LIMIT 0 1` to find the least loaded active worker.
- If a target worker is selected, the job is pushed to that worker's private queue `task_engine:worker:queue:<worker_id>`.
- Workers prioritize their private queue before falling back to the global priority queue, enabling targeted execution without starving general worker pools.

### 7.3 Global Concurrency Limits per Job Type
- To prevent heavy ML tasks (e.g. `model_training`) from saturating cluster resources, the engine enforces active concurrency caps.
- Monitored via Redis keys `task_engine:concurrency:active:<job_type>`.
- When claiming a job, the worker attempts an atomic `INCR` guarded by `acquire_concurrency_slot(...)`. If saturated, the job is deferred (re-queued) without counting against failure retries.
- On completion (or failure/timeout), `release_concurrency_slot(...)` executes `DECR` to yield the slot.

### 7.4 Sliding Window Rate Limiting (Token / Call Throttling)
- Implemented using Redis Sorted Sets with a sub-millisecond atomic Lua script.
- Enforces requests-per-window quotas (e.g. 5 calls per 60s for external APIs or LLM inference endpoints).
- Members are formatted as `<timestamp>:<seq>` to guarantee element uniqueness within identical microsecond execution intervals.

## 8. Phase 7 Design Decisions: Job Dependencies (DAG Execution)

### 8.1 Cycle Detection via DFS Graph Coloring
- Validates batch dependency graphs and incremental parent relationships using 3-color DFS cycle detection ($0$ = unvisited, $1$ = in recursion stack, $2$ = completed).
- Self-dependencies ($A \to A$) and multi-hop circular paths ($A \to B \to C \to A$) are rejected upfront with HTTP 400 Bad Request before database persistence.

### 8.2 Dependency Gatekeeper & Dynamic Unblocking
- **Initial Gate**: If a job specifies `depends_on`, it starts in `PENDING` status and is NOT enqueued into Redis.
- **Completion Hook**: When a parent job transitions to `SUCCEEDED`, the worker triggers `DAGManager.on_job_succeeded(...)`:
  - Queries all dependent children where `depends_on_job_id == parent.id`.
  - Atomically verifies if all prerequisite parent jobs have reached `SUCCEEDED`.
  - Once satisfied, the child is promoted from `PENDING` to `QUEUED` and pushed into the Redis priority queue.

### 8.3 Fail-Fast Cascading
- If any parent job terminates abnormally (`FAILED`, `CANCELLED`, or `DEAD`), downstream dependents cannot proceed.
- Downstream jobs immediately transition to `FAILED` with explicit root-cause error diagnostics (`Dependency failure: parent job [...] terminated with status [...]`).
- Prevents wasted execution cycles and ensures atomic pipeline integrity.

## 9. Phase 8 Design Decisions: Prometheus Metrics & Structured Logging

### 9.1 Prometheus Observability Model
Exposed via `GET /metrics` and `GET /api/v1/metrics` using `prometheus_client`:
- **Queue Depths (`task_engine_queue_depth{queue=...}`)**: Sampled directly from Redis sorted sets (`priority`, `delayed`, `dlq`) on scrape.
- **Active Workers (`task_engine_active_workers_count`)**: Evaluates real-time heartbeats within the healthy threshold window ($< 15\text{s}$).
- **Throughput & States (`task_engine_jobs_submitted_total`, `task_engine_jobs_completed_total{status=...}`)**: Partitioned by registered `job_type` and terminal status (`SUCCEEDED`, `FAILED`, `TIMED_OUT`, `DEAD`, `CANCELLED`).
- **High-Precision Histograms**:
  - `task_engine_job_execution_duration_seconds`: Bucketed from 5ms up to 60s to enable p50, p95, and p99 query evaluation in Prometheus/Grafana.
  - `task_engine_job_scheduling_latency_seconds`: Measures exact queue dwell time from job creation to worker execution start.

### 9.2 Context-Aware Structured JSON Logging
- **`contextvars` Architecture**: Uses Python's native `ContextVar` to propagate request, job, and worker context across asynchronous coroutines without polluting function signatures.
- **`StructuredJSONFormatter`**: Emits compact, machine-ingestible JSON with fields:
  `{"timestamp": "...", "level": "INFO", "name": "...", "message": "...", "request_id": "...", "job_id": "...", "worker_id": "..."}`
- **Correlation ID Middleware**: Automatically extracts or generates `X-Request-ID` on incoming HTTP requests, binds it to the context, and echoes both `X-Request-ID` and `X-Response-Time-Ms` in HTTP response headers.

## 10. Phase 9 Design Decisions: Chaos Engineering & Fault Tolerance

### 10.1 Worker Mid-Execution Crash Recovery
- **Failure Simulation**: Abrupt termination of an in-flight worker node mid-execution (simulating kernel panics, OOM kills, or power cuts).
- **At-Least-Once Delivery**:
  - Unresponsive node detected via missed Redis/Postgres heartbeats.
  - In-flight jobs locked via `SELECT ... FOR UPDATE`, attempt recorded documenting crash state, status restored to `QUEUED`, and job re-enqueued to Redis.
  - Successor workers transparently claim and execute the job to completion with zero job loss.

### 10.2 Transient Redis Partition Tolerance
- The engine handles transient Redis disconnects gracefully: workers catch connection failures, back off, and re-establish connection pool health once connectivity recovers.
- All transactional state and audit history remain durably persisted in PostgreSQL.

## 11. Phase 10 Design Decisions: High-Concurrency Load Testing

### 11.1 Benchmark Topology
- **Concurrency**: 50 parallel client coroutines submitting 1,000 distinct jobs across mixed types (`model_inference`, `batch_scoring`, `sleep`, `model_training`) with random priorities (0-9).
- **Execution Fleet**: 6 worker process daemons (4 execution slots each = 24 parallel execution slots).
- **Tooling**: Locust workload suite (`benchmarks/locustfile.py`) and high-throughput async runner (`benchmarks/load_test_runner.py`).

### 11.2 Measured Performance Metrics
All metrics extracted directly from live PostgreSQL and Redis runtime measurements (stored in `benchmarks/results.json`):
- **Total Jobs**: 1,000 submitted / 1,000 completed.
- **Success Rate**: 100.0% (0 failures, 0 lost jobs).
- **Submission Throughput**: 25.4 req/s.
- **Processing Throughput**: 29.8 jobs/s.
- **Scheduling Dwell Latency**: p50 = 29.48s, p95 = 56.07s, p99 = 62.38s (governed by 24 slot queue drain rate).
- **Task Execution Duration**: p50 = 47.0ms, p95 = 1,031.0ms, p99 = 1,047.0ms.





