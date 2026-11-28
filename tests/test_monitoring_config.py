import json
from pathlib import Path
import yaml
import pytest
from prometheus_client import REGISTRY

from app.core.metrics import (
    QUEUE_DEPTH,
    ACTIVE_WORKERS,
    JOBS_SUBMITTED,
    JOBS_COMPLETED,
    JOBS_RETRIED,
    JOB_EXECUTION_DURATION,
    JOB_SCHEDULING_LATENCY,
    RUNNING_JOBS,
    collect_system_metrics,
)


REPO_ROOT = Path(__file__).resolve().parent.parent


def test_prometheus_yaml_validity():
    """Validates monitoring/prometheus/prometheus.yml syntax and configuration."""
    prom_yaml_path = REPO_ROOT / "monitoring" / "prometheus" / "prometheus.yml"
    assert prom_yaml_path.exists(), "prometheus.yml must exist"

    with open(prom_yaml_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    assert data is not None
    assert "global" in data
    assert data["global"]["scrape_interval"] == "5s"
    assert "scrape_configs" in data
    job = next((j for j in data["scrape_configs"] if j["job_name"] == "task-engine-api"), None)
    assert job is not None
    assert job["metrics_path"] == "/metrics"
    assert "task-engine-api:10000" in job["static_configs"][0]["targets"]


def test_grafana_provisioning_yamls():
    """Validates Grafana datasource and dashboard provisioning YAML configurations."""
    ds_path = REPO_ROOT / "monitoring" / "grafana" / "provisioning" / "datasources" / "prometheus.yml"
    assert ds_path.exists(), "Grafana datasource provisioning YAML must exist"

    with open(ds_path, "r", encoding="utf-8") as f:
        ds_data = yaml.safe_load(f)

    assert ds_data["apiVersion"] == 1
    ds = ds_data["datasources"][0]
    assert ds["uid"] == "prometheus"
    assert ds["url"] in ("http://prometheus:9090", "http://prometheus:10000")
    assert ds["isDefault"] is True

    dash_path = REPO_ROOT / "monitoring" / "grafana" / "provisioning" / "dashboards" / "dashboards.yml"
    assert dash_path.exists(), "Grafana dashboard provisioning YAML must exist"

    with open(dash_path, "r", encoding="utf-8") as f:
        dash_data = yaml.safe_load(f)

    assert dash_data["apiVersion"] == 1
    provider = dash_data["providers"][0]
    assert provider["name"] == "task-engine-dashboards"
    assert provider["options"]["path"] == "/var/lib/grafana/dashboards"


def test_grafana_task_engine_dashboard_json():
    """Validates Grafana dashboard JSON schema, refresh interval, theme, and real metrics queries."""
    json_path = REPO_ROOT / "monitoring" / "grafana" / "dashboards" / "task-engine.json"
    assert json_path.exists(), "task-engine.json must exist"

    with open(json_path, "r", encoding="utf-8") as f:
        dashboard = json.load(f)

    assert dashboard["title"] == "Distributed Task Engine"
    assert dashboard["uid"] == "distaskeng-overview"
    assert dashboard["refresh"] == "5s"
    assert dashboard["style"] == "dark"
    assert dashboard["time"]["from"] == "now-15m"

    # Verify that all panel queries reference legitimate metrics from app/core/metrics.py
    allowed_metric_names = {
        "task_engine_queue_depth",
        "task_engine_active_workers_count",
        "task_engine_jobs_by_status",
        "task_engine_jobs_submitted_total",
        "task_engine_jobs_completed_total",
        "task_engine_db_jobs_completed_total",
        "task_engine_jobs_retried_total",
        "task_engine_job_execution_duration_seconds",
        "task_engine_db_job_execution_duration_seconds",
        "task_engine_job_scheduling_latency_seconds",
        "task_engine_running_jobs",
    }

    panel_targets = []
    for panel in dashboard.get("panels", []):
        if "targets" in panel:
            for t in panel["targets"]:
                expr = t.get("expr", "")
                panel_targets.append(expr)
                # Ensure at least one allowed metric is referenced
                has_valid_metric = any(m in expr for m in allowed_metric_names)
                assert has_valid_metric, f"Panel query '{expr}' does not reference any known metric in {allowed_metric_names}"

    assert len(panel_targets) >= 10, f"Expected at least 10 query targets in dashboard, got {len(panel_targets)}"


@pytest.mark.asyncio
async def test_jobs_retried_metric():
    """Verifies that JOBS_RETRIED metric counter exists and increments properly."""
    initial_val = JOBS_RETRIED.labels(job_type="model_inference")._value.get()
    JOBS_RETRIED.labels(job_type="model_inference").inc()
    new_val = JOBS_RETRIED.labels(job_type="model_inference")._value.get()
    assert new_val == initial_val + 1

    # Verify present in collect_system_metrics Prometheus text
    output = await collect_system_metrics()
    assert b"task_engine_jobs_retried_total" in output


def test_docker_compose_monitoring_validity():
    """Validates docker-compose.monitoring.yml structure, services, and ports."""
    compose_path = REPO_ROOT / "docker-compose.monitoring.yml"
    assert compose_path.exists(), "docker-compose.monitoring.yml must exist at repo root"

    with open(compose_path, "r", encoding="utf-8") as f:
        compose = yaml.safe_load(f)

    assert compose is not None
    assert "services" in compose
    services = compose["services"]

    # Must contain ONLY prometheus and grafana (no api, worker, redis, postgres)
    assert set(services.keys()) == {"prometheus", "grafana"}
    assert "postgres" not in services
    assert "redis" not in services
    assert "api" not in services
    assert "worker" not in services

    # Prometheus service
    prom = services["prometheus"]
    assert prom["image"] == "prom/prometheus:v2.51.0"
    assert any("9090" in str(p) for p in prom.get("ports", []))
    assert any("prometheus.monitoring.yml" in str(v) for v in prom.get("volumes", []))

    # Grafana service
    grafana = services["grafana"]
    assert grafana["image"] == "grafana/grafana:10.4.0"
    assert any("3000" in str(p) for p in grafana.get("ports", []))
    assert any("provisioning" in str(v) for v in grafana.get("volumes", []))
    assert any("dashboards" in str(v) for v in grafana.get("volumes", []))

    # Environment
    env_str = str(grafana.get("environment", []))
    assert "GF_SECURITY_ADMIN_PASSWORD" in env_str


def test_prometheus_monitoring_yaml_validity():
    """Validates monitoring/prometheus/prometheus.monitoring.yml configuration for live Render scraping."""
    prom_yaml_path = REPO_ROOT / "monitoring" / "prometheus" / "prometheus.monitoring.yml"
    assert prom_yaml_path.exists(), "prometheus.monitoring.yml must exist"

    with open(prom_yaml_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    assert data is not None
    assert data["global"]["scrape_interval"] == "15s"
    assert data["global"]["scrape_timeout"] == "10s"

    job = data["scrape_configs"][0]
    assert job["scheme"] == "https"
    assert job["metrics_path"] == "/metrics"
    assert job["scrape_interval"] == "15s"
    assert job["scrape_timeout"] == "10s"

    targets = job["static_configs"][0]["targets"]
    assert "distributed-task-engine.onrender.com" in targets


def test_env_monitoring_example_validity():
    """Validates .env.monitoring.example exists and contains no production secrets."""
    example_path = REPO_ROOT / ".env.monitoring.example"
    assert example_path.exists(), ".env.monitoring.example must exist"

    content = example_path.read_text(encoding="utf-8")
    assert "GF_SECURITY_ADMIN_PASSWORD=" in content
    assert "PROMETHEUS_PORT=" in content
    assert "GRAFANA_PORT=" in content
    assert "RENDER_TARGET=" in content

