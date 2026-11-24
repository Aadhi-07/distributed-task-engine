import uuid
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_submit_job_api_success(async_client: AsyncClient):
    payload = {
        "job_type": "model_training",
        "payload": {"epochs": 3, "dataset": "cifar10"},
        "priority": 7,
        "timeout_seconds": 120,
        "max_retries": 2,
    }
    response = await async_client.post("/jobs", json=payload)
    assert response.status_code == 201
    data = response.json()

    assert data["job_type"] == "model_training"
    assert data["status"] == "QUEUED"
    assert data["priority"] == 7
    assert data["timeout_seconds"] == 120
    assert data["max_retries"] == 2
    assert "id" in data
    assert data["result"] is None

    # Verify queryable via GET /jobs/{id}
    job_id = data["id"]
    get_res = await async_client.get(f"/jobs/{job_id}")
    assert get_res.status_code == 200
    assert get_res.json()["id"] == job_id


@pytest.mark.asyncio
async def test_submit_job_idempotency(async_client: AsyncClient):
    idem_key = f"idem-test-{uuid.uuid4().hex}"
    payload = {
        "job_type": "batch_scoring",
        "payload": {"batch_size": 50},
        "idempotency_key": idem_key,
    }

    # First submission
    first_res = await async_client.post("/jobs", json=payload)
    assert first_res.status_code == 201
    first_data = first_res.json()
    job_id = first_data["id"]

    # Second submission with same idempotency key
    second_res = await async_client.post("/jobs", json=payload)
    assert second_res.status_code == 200
    second_data = second_res.json()

    assert second_data["id"] == job_id
    assert second_data["idempotency_key"] == idem_key


@pytest.mark.asyncio
async def test_get_nonexistent_job_returns_404(async_client: AsyncClient):
    non_existent_id = uuid.uuid4()
    response = await async_client.get(f"/jobs/{non_existent_id}")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_list_jobs_and_filtering(async_client: AsyncClient):
    # Submit two distinct jobs
    await async_client.post("/jobs", json={
        "job_type": "model_inference",
        "payload": {"model": "bert"},
    })
    await async_client.post("/jobs", json={
        "job_type": "sleep",
        "payload": {"seconds": 0.1},
    })

    # Query all
    res = await async_client.get("/jobs?limit=10")
    assert res.status_code == 200
    data = res.json()
    assert "items" in data
    assert data["total"] >= 2
    assert len(data["items"]) <= 10

    # Query with filter
    filter_res = await async_client.get("/jobs?job_type=model_inference")
    assert filter_res.status_code == 200
    filter_data = filter_res.json()
    assert any(item["job_type"] == "model_inference" for item in filter_data["items"])
