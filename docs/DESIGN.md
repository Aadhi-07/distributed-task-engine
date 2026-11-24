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
