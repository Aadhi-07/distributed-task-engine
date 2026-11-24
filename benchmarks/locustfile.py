import random
from locust import HttpUser, task, between


class TaskEngineLoadUser(HttpUser):
    """Simulates high-concurrency client submitting varied workloads."""
    wait_time = between(0.01, 0.05)

    @task(5)
    def submit_inference_workload(self):
        self.client.post(
            "/jobs",
            json={
                "job_type": "model_inference",
                "payload": {"features": [random.random() for _ in range(8)]},
                "priority": random.randint(0, 9),
                "timeout_seconds": 60,
                "max_retries": 1,
            },
            name="POST /jobs [inference]",
        )

    @task(3)
    def submit_batch_scoring(self):
        self.client.post(
            "/jobs",
            json={
                "job_type": "batch_scoring",
                "payload": {"dataset": "eval_split", "batch_size": random.randint(10, 50)},
                "priority": random.randint(3, 8),
                "timeout_seconds": 120,
                "max_retries": 2,
            },
            name="POST /jobs [batch_scoring]",
        )

    @task(2)
    def submit_lightweight_job(self):
        self.client.post(
            "/jobs",
            json={
                "job_type": "sleep",
                "payload": {"duration": 0.005},
                "priority": random.randint(5, 9),
            },
            name="POST /jobs [sleep]",
        )

    @task(1)
    def scrape_metrics(self):
        self.client.get("/metrics", name="GET /metrics")

    @task(1)
    def list_workers(self):
        self.client.get("/workers", name="GET /workers")
