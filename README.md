# Distributed Task Execution Engine

A production-grade, fault-tolerant distributed task execution platform written in Python 3.12, FastAPI, Redis 7, and PostgreSQL 16. Designed for reliable execution of compute and I/O workloads (such as ML model training, inference, and batch scoring) with priority scheduling, atomic worker claims, exponential backoff with jitter, dead worker recovery, DAG dependencies, load balancing, and Prometheus observability.

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph Clients["Clients & SDK"]
        API_CLIENT["HTTP Client / CLI / SDK"]
    end

    subgraph API_Tier["API Tier (FastAPI)"]
        API["FastAPI Cluster (Uvicorn)"]
        ROUTER["API Router (/jobs, /workers, /metrics)"]
        DAG_VAL["DAG Cycle Detector (DFS 3-Color)"]
    end

    subgraph Coordination["Coordination Layer (Redis 7)"]
        PRIO_QUEUE[("Priority ZSET\nScore = (9-P)*1e9 + ts")]
        DELAYED_QUEUE[("Delayed Retries ZSET\nScore = Due Timestamp")]
        IN_FLIGHT_HASH[("Processing Hash\nWorker:StartedAt")]
        DLQ_ZSET[("Dead Letter Queue (DLQ)")]
        HEARTBEATS_ZSET[("Worker Heartbeats ZSET")]
        LOAD_ZSET[("Worker Load Score ZSET")]
    end

    subgraph Workers["Worker Fleet (Asyncio Daemons)"]
        W1["Worker 1 (Concurrency: 4)"]
        W2["Worker 2 (Concurrency: 4)"]
        WN["Worker N (Concurrency: 4)"]
    end

    subgraph Persistence["Persistence Layer (PostgreSQL 16)"]
        DB_JOBS[("jobs (State Machine, Parameters, Status)")]
        DB_ATTEMPTS[("job_attempts (Immutable History, Logs, Latency)")]
        DB_DEPS[("job_dependencies (DAG Edges)")]
        DB_WORKERS[("workers (Node Health, CPU, RAM)")]
    end

    API_CLIENT -->|HTTP POST /jobs| API
    API --> ROUTER
    ROUTER --> DAG_VAL
    ROUTER -->|Persist Job & Attempts| Persistence
    ROUTER -->|ZADD Priority Score| PRIO_QUEUE

    W1 & W2 & WN -->|Atomic Lua Claim ZPOPMIN| PRIO_QUEUE
    PRIO_QUEUE -.->|Move to Processing Hash| IN_FLIGHT_HASH
    W1 & W2 & WN -->|SELECT FOR UPDATE Claim| Persistence
    W1 & W2 & WN -->|Heartbeat Ping 5s| HEARTBEATS_ZSET
    W1 & W2 & WN -->|Report CPU/RAM/Load| LOAD_ZSET
    W1 & W2 & WN -->|On Failure: ZADD Retry| DELAYED_QUEUE
    W1 & W2 & WN -->|On Exhaustion: ZADD DLQ| DLQ_ZSET
```

---

## Design Decisions & Tradeoffs

### 1. Why Redis Sorted Sets (ZSET) for Priority Queues?
Traditional FIFO queues (like Redis `LPUSH`/`BRPOP` or RabbitMQ standard queues) require separate queues for each priority tier ($10$ queues for priorities $0-9$). This introduces queue starvation, complex polling logic, and uneven worker utilization.
Instead, we use a single Redis Sorted Set (`task_engine:queue:priority`) scored with:
$$\text{Score} = (9 - \text{priority}) \times 10^9 + (\text{epoch\_seconds} \bmod 10^9)$$

- **Mathematical Dominance**: Priority $9$ yields offset $0$; Priority $8$ yields $1 \times 10^9$; Priority $0$ yields $9 \times 10^9$. Because $10^9$ is larger than any modulo time offset, high priority jobs are mathematically guaranteed to have a lower score than lower priority jobs.
- **Strict FIFO Tie-Breaking**: For identical priorities, older jobs have smaller timestamps, guaranteeing deterministic FIFO ordering via `ZPOPMIN`.
- **$O(\log N)$ Ingestion & $O(1)$ Dequeue**: Atomic Lua scripting claims jobs from the sorted set directly into an in-flight hash in sub-millisecond single-threaded Redis operations.

### 2. At-Least-Once vs. Exactly-Once Delivery
In distributed systems subject to network partitions, node crashes, and arbitrary worker timeouts, **true exactly-once execution across network boundaries is impossible without distributed two-phase commits (which severely degrade throughput and availability)**.
We intentionally implement **At-Least-Once Delivery backed by Idempotency Keys**:
1. **At-Least-Once**:
   - If a worker crashes mid-task, the dead worker detection subsystem identifies missed heartbeats ($>15\text{s}$), records a failed crash attempt, resets the job status to `QUEUED`, and re-enqueues it.
2. **Idempotency Safeguard**:
   - Every job submission supports a client-provided `idempotency_key` backed by a unique PostgreSQL index (`uq_jobs_idempotency_key`).
   - Duplicate submissions immediately return the existing job with HTTP 200 without enqueuing duplicates.
   - Tasks that produce external side effects use this key to avoid duplicate mutations.

### 3. How Race Conditions are Avoided
Race conditions are eliminated across both the coordination and persistence tiers:
- **Atomic Dequeue (Redis Lua Script)**:
  `ZPOPMIN` and `HSET task_engine:processing <job_id> <worker_id>:<ts>` are executed within a single Redis Lua script. Multiple concurrent workers can never pull or claim the same job member simultaneously.
- **Transactional Row Locking (PostgreSQL `SELECT ... FOR UPDATE`)**:
  Before transitioning a job to `RUNNING`, the worker locks the row with `SELECT ... FOR UPDATE`. If another process has already modified the job status (e.g. cancelled by user), the claim aborts cleanly.
- **Cycle-Free DAG Ingestion**:
  DAG batch submissions and incremental edge additions perform 3-color DFS graph coloring (`find_cycle_in_graph`) before database persistence, rejecting circular dependencies upfront with HTTP 400.
- **Atomic Concurrency Quotas**:
  Active job concurrency caps per job type (`task_engine:concurrency:active:<job_type>`) and sliding-window rate limiters use atomic Redis increment and score filtering, preventing race oversubscriptions.

---

## Real Benchmark Results (1,000 Concurrent Jobs)

The platform was benchmarked by submitting **1,000 concurrent jobs** with mixed workloads (`model_inference`, `batch_scoring`, `sleep`, `model_training`) across 50 concurrent client workers against a 6-worker execution fleet (24 concurrency slots).

All numbers are real, measured metrics extracted directly from PostgreSQL and Redis (saved in `benchmarks/results.json`):

| Metric | Result | Notes |
| :--- | :--- | :--- |
| **Total Jobs Submitted** | **1,000** | 100% submission success |
| **Total Jobs Completed** | **1,000** | 100% completion success |
| **Failure Rate** | **0.00%** | Zero lost or dropped jobs |
| **Submission Throughput** | **25.4 req/s** | 1,000 jobs submitted in 39.41s |
| **Processing Throughput** | **29.8 jobs/s** | Drained 1,000 jobs in 33.58s across 24 slots |
| **Scheduling Latency (p50)** | **29.48 s** | Queue dwell time under sustained queue saturation |
| **Scheduling Latency (p95)** | **56.07 s** | Queue dwell time for lowest priority tail |
| **Scheduling Latency (p99)** | **62.38 s** | Near-tail drain latency |
| **Task Execution Time (p50)** | **47.0 ms** | Fast inference and sleep tasks |
| **Task Execution Time (p95)** | **1,031.0 ms** | Batch scoring and ML training epochs |
| **Task Execution Time (p99)** | **1,047.0 ms** | Maximum workload compute duration |

---

## Quickstart & Local Setup

### Option A: Local Development (.env)

#### Prerequisites
- Python 3.12+
- Local PostgreSQL (`localhost:5432`)
- Local Redis (`localhost:6379`)

#### 1. Clone & Configure Environment
```bash
git clone https://github.com/example/distaskeng.git
cd distaskeng

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

#### 2. Create `.env`
```ini
ENV=development
DEBUG=true
LOG_LEVEL=INFO

# Database
DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/taskdb
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=taskdb

# Redis
REDIS_URL=redis://localhost:6379/0
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

# Worker Settings
WORKER_CONCURRENCY=4
WORKER_HEARTBEAT_INTERVAL=5
WORKER_DEAD_THRESHOLD=15
```

#### 3. Run Migrations & Start Services
```bash
# Apply database schema migrations
alembic upgrade head

# Start API Server (in terminal 1)
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# Start Worker Daemon (in terminal 2)
python -m app.worker
```

---

### Option B: Docker Compose

To start the entire platform with PostgreSQL, Redis, auto-migrations, FastAPI, and worker daemons:

```bash
docker compose up -d --build
```

Check health status:
```bash
docker compose ps
curl http://localhost:8000/health/ready
```

---

## API Documentation & Examples

### 1. Submit a Job (`POST /jobs`)
```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "job_type": "model_inference",
    "payload": {
      "model": "resnet50",
      "features": [0.12, 0.45, 0.88, 0.23]
    },
    "priority": 8,
    "timeout_seconds": 60,
    "max_retries": 3,
    "idempotency_key": "inf-req-1001"
  }'
```

### 2. Submit a DAG Workflow (`POST /jobs/dag`)
```bash
curl -X POST http://localhost:8000/jobs/dag \
  -H "Content-Type: application/json" \
  -d '{
    "nodes": [
      {
        "node_id": "extract",
        "job_type": "sleep",
        "payload": {"duration": 0.1},
        "priority": 9
      },
      {
        "node_id": "train",
        "job_type": "model_training",
        "payload": {"epochs": 3},
        "priority": 7,
        "depends_on": ["extract"]
      },
      {
        "node_id": "evaluate",
        "job_type": "model_inference",
        "payload": {"features": [1.0, 2.0]},
        "priority": 5,
        "depends_on": ["train"]
      }
    ]
  }'
```

### 3. Inspect Job Status & Execution History (`GET /jobs/{id}`)
```bash
curl http://localhost:8000/jobs/c7111767-1b40-47d1-addd-167bb70565b2
```

### 4. Cancel a Running or Queued Job (`POST /jobs/{id}/cancel`)
```bash
curl -X POST http://localhost:8000/jobs/c7111767-1b40-47d1-addd-167bb70565b2/cancel
```

### 5. Inspect Worker Cluster Health & Hardware Load (`GET /workers`)
```bash
curl http://localhost:8000/workers
```

### 6. Scrape Prometheus Metrics (`GET /metrics`)
```bash
curl http://localhost:8000/metrics
```

Example Prometheus output:
```text
# HELP task_engine_queue_depth Current depth of task engine queues
# TYPE task_engine_queue_depth gauge
task_engine_queue_depth{queue="priority"} 0.0
task_engine_queue_depth{queue="delayed"} 0.0
task_engine_queue_depth{queue="dlq"} 0.0

# HELP task_engine_active_workers_count Number of active healthy workers heartbeating
# TYPE task_engine_active_workers_count gauge
task_engine_active_workers_count 6.0

# HELP task_engine_jobs_completed_total Total number of completed jobs by status
# TYPE task_engine_jobs_completed_total counter
task_engine_jobs_completed_total{job_type="model_inference",status="SUCCEEDED"} 550.0
task_engine_jobs_completed_total{job_type="batch_scoring",status="SUCCEEDED"} 250.0
task_engine_jobs_completed_total{job_type="sleep",status="SUCCEEDED"} 150.0
task_engine_jobs_completed_total{job_type="model_training",status="SUCCEEDED"} 50.0
```

---

## Real-Time Visual Dashboard

The platform includes a real-time visual monitoring dashboard inspired by a dark "policy knowledge graph" interface. Served via plain HTML, CSS, and vanilla JavaScript (with pinned D3.js v7 for the force-directed topology layout) directly from FastAPI StaticFiles at `/dashboard`.

```mermaid
flowchart LR
    subgraph Browser["Browser Client (/dashboard)"]
        UI["Vanilla JS + D3 v7 Force Graph\n(Top Bar, Inspector, Event Feed)"]
        POLL["1-Second Polling Loop\n(If-None-Match: <etag>)"]
    end

    subgraph API["FastAPI Tier"]
        SNAP["GET /api/v1/dashboard/snapshot"]
        ETAG_CHECK{"ETag Match?"}
        NOT_MOD["304 Not Modified\n(Minimal CPU & Network)"]
        SERVICE["DashboardService\n(Strict Read-Only Aggregator)"]
    end

    subgraph Storage["Persistence & Coordination"]
        PG[("PostgreSQL 16\n(Jobs, Attempts, Workers, DAGs)")]
        RD[("Redis 7\n(Queue Depths, Delayed, DLQ)")]
    end

    UI --> POLL
    POLL -->|GET /snapshot + If-None-Match| SNAP
    SNAP --> ETAG_CHECK
    ETAG_CHECK -->|Unchanged State| NOT_MOD
    NOT_MOD -.->|HTTP 304| UI
    ETAG_CHECK -->|State Changed| SERVICE
    SERVICE -->|Optimized indexed queries| PG
    SERVICE -->|Atomic ZCARD queue metrics| RD
    SERVICE -->|HTTP 200 + Snapshot JSON + ETag| UI
```

### Visual & Semantic Architecture
- **Central Queue Hub**: Represents the scheduler and priority queue with live depth counters.
- **Worker Orbital Ring**: Active workers arranged in an orbit around the hub; node radius encodes load score and ring color reflects health (`#4ADE80` healthy, `#FBBF24` stale heartbeat, `#F87171` dead).
- **Job Nodes**: Queued jobs cluster around the hub; running jobs attach to their executing worker; finished jobs fade outward.
- **Dynamic Edges**:
  - **Queue Edges**: Subtle dashed grey edges from hub to queued jobs.
  - **Assignment Edges**: Flowing pulsing cyan edges connecting workers to in-flight jobs.
  - **DAG Dependencies**: Directed arrows encoding parent-child relationships. Red dashed edges (`#F87171`) explicitly highlight jobs blocked by failed parents.
- **Top Metrics Strip**: Real-time queue depth breakdown (priority, delayed, DLQ), throughput (jobs/s), p50/p95/p99 latency (ms), failure rate %, and alive worker capacity.
- **Right Inspector Panel**: Click any node to inspect complete metadata, execution duration, full attempt history, parent/child DAG dependencies, input/output payload, and error stack traces.
- **Bottom Event Feed**: Auto-scrolling real-time log of the last 50 state transitions (`QUEUED`, `RUNNING`, `SUCCEEDED`, `RETRY`, `DEAD`, `WORKER_LOST`, `REQUEUED`).
- **Filters & Search**: Filter by status pills, job type, minimum priority threshold (0-9), or free-text search across job IDs, worker IDs, and error messages.

### How to Run the Dashboard

1. **Start the API server**:
   ```bash
   uvicorn app.main:app --reload
   ```

2. **Start one or more workers**:
   ```bash
   python -m app.worker
   ```

3. **Open the Dashboard in your browser**:
   ```text
   http://localhost:8000/dashboard
   ```

4. **Run the Live Demo Workload Generator**:
   In another terminal, run `scripts/demo.py` to submit a diamond DAG with a deliberately failing branch alongside a burst of mixed-priority workloads:
   ```bash
   # Submit a Diamond DAG + 30 mixed-priority jobs
   python scripts/demo.py --burst 30

   # Or run with an ephemeral worker to observe live execution cycles in the UI
   python scripts/demo.py --burst 30 --run-worker
   ```

> [!IMPORTANT]
> **Security Notice**: The visual dashboard is strictly **unauthenticated** and is designed specifically for **local development, observability, and debugging**. It operates in strict read-only mode against the engine and cannot mutate job states. Do not expose `/dashboard` or the snapshot endpoint directly to public networks without placing them behind an authenticating reverse proxy or API gateway.

---

## Testing & Quality Assurance

Run the complete test suite:
```bash
pytest -v
```

Run chaos engineering scenarios (worker death mid-execution & Redis disconnect):
```bash
pytest -v tests/test_chaos.py
```

Run 1,000+ job load benchmark:
```bash
python benchmarks/load_test_runner.py
```

Run Locust interactive load test:
```bash
locust -f benchmarks/locustfile.py --host http://localhost:8000
```

---

## License
MIT License.
