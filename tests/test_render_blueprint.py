from pathlib import Path
import yaml
import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent


def test_render_yaml_validity_and_schema():
    """Validates render.yaml syntax and Render Blueprint specification requirements."""
    render_yaml_path = REPO_ROOT / "render.yaml"
    assert render_yaml_path.exists(), "render.yaml must exist at repo root"

    with open(render_yaml_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    assert data is not None
    assert "services" in data
    assert "databases" in data

    services = {s["name"]: s for s in data["services"]}
    databases = {d["name"]: d for d in data["databases"]}

    # 1. Database
    assert "task-engine-db" in databases
    assert databases["task-engine-db"]["plan"] == "free"

    # 2. Web Service: task-engine-api
    api = services.get("task-engine-api")
    assert api is not None
    assert api["type"] == "web"
    assert api["env"] == "docker"
    assert api["dockerfilePath"] == "./docker/Dockerfile.api"
    assert api["healthCheckPath"] == "/health/live"

    api_env_keys = {e["key"]: e for e in api["envVars"]}
    assert "DATABASE_URL" in api_env_keys
    assert api_env_keys["DATABASE_URL"]["fromDatabase"]["name"] == "task-engine-db"
    assert api_env_keys["DATABASE_URL"]["fromDatabase"]["property"] == "connectionString"

    assert "REDIS_URL" in api_env_keys
    assert api_env_keys["REDIS_URL"]["fromService"]["type"] == "keyvalue"
    assert api_env_keys["REDIS_URL"]["fromService"]["name"] == "task-engine-redis"
    assert api_env_keys["REDIS_URL"]["fromService"]["property"] == "connectionString"

    assert "API_KEY" in api_env_keys
    assert api_env_keys["API_KEY"]["sync"] is False

    # 3. Worker: task-engine-worker
    worker = services.get("task-engine-worker")
    assert worker is not None
    assert worker["type"] == "worker"
    assert worker["env"] == "docker"
    assert worker["dockerfilePath"] == "./docker/Dockerfile.worker"
    assert worker["numInstances"] == 3

    worker_env_keys = {e["key"]: e for e in worker["envVars"]}
    assert "DATABASE_URL" in worker_env_keys
    assert "REDIS_URL" in worker_env_keys
    assert worker_env_keys["WORKER_CONCURRENCY"]["value"] == "4"

    # 4. Key Value: task-engine-redis
    redis = services.get("task-engine-redis")
    assert redis is not None
    assert redis["type"] == "keyvalue"
    assert redis["plan"] == "free"
    assert redis["ipAllowList"] == []

    # 5. Private Service: prometheus
    prom = services.get("prometheus")
    assert prom is not None
    assert prom["type"] == "pserv"
    assert prom["env"] == "docker"
    assert prom["dockerContext"] == "./monitoring/prometheus"

    # 6. Web Service: grafana
    grafana = services.get("grafana")
    assert grafana is not None
    assert grafana["type"] == "web"
    assert grafana["env"] == "docker"
    assert grafana["dockerContext"] == "./monitoring/grafana"

    grafana_env_keys = {e["key"]: e for e in grafana["envVars"]}
    assert "GF_SECURITY_ADMIN_PASSWORD" in grafana_env_keys
    assert grafana_env_keys["GF_SECURITY_ADMIN_PASSWORD"]["sync"] is False
    assert grafana_env_keys["GF_SERVER_HTTP_PORT"]["value"] == "10000"
