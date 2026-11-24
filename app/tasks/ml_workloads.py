import asyncio
import math
import random
import time
from typing import Any, Dict
from app.tasks.registry import register_task


@register_task("model_training")
async def task_model_training(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Simulates deep learning model training over N epochs."""
    epochs = payload.get("epochs", 5)
    dataset = payload.get("dataset", "mnist")
    learning_rate = payload.get("learning_rate", 0.001)

    loss = 1.0
    accuracy = 0.50

    for epoch in range(1, epochs + 1):
        # Simulate CPU/GPU compute cycle
        await asyncio.sleep(0.05)
        loss = round(loss * 0.7 + random.uniform(0.01, 0.05), 4)
        accuracy = round(min(0.99, accuracy + (1.0 - accuracy) * 0.3), 4)

    return {
        "status": "completed",
        "dataset": dataset,
        "epochs_trained": epochs,
        "final_loss": loss,
        "final_accuracy": accuracy,
        "weights_artifact": f"s3://models/{dataset}/checkpoint_epoch_{epochs}.pt",
    }


@register_task("model_inference")
async def task_model_inference(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Simulates model inference / scoring for given input items."""
    model_name = payload.get("model", "resnet50")
    inputs = payload.get("inputs", [{"id": 1, "features": [0.1, 0.5, 0.9]}])

    # Simulate inference latency
    await asyncio.sleep(0.02)

    predictions = []
    for item in inputs:
        item_id = item.get("id", random.randint(1000, 9999))
        confidence = round(random.uniform(0.85, 0.99), 4)
        predictions.append({
            "id": item_id,
            "predicted_class": random.choice(["cat", "dog", "automobile", "airplane"]),
            "confidence": confidence,
        })

    return {
        "model": model_name,
        "items_processed": len(inputs),
        "predictions": predictions,
    }


@register_task("batch_scoring")
async def task_batch_scoring(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Simulates batch scoring pipeline over a partitioned dataset."""
    batch_size = payload.get("batch_size", 100)
    partition_id = payload.get("partition_id", "part-0000")

    await asyncio.sleep(0.03)

    return {
        "partition_id": partition_id,
        "records_scored": batch_size,
        "mean_score": round(random.uniform(70.0, 95.0), 2),
        "output_location": f"s3://analytics/scored/{partition_id}.parquet",
    }


@register_task("sleep")
async def task_sleep(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Simulates a task with specified duration (seconds)."""
    duration = payload.get("seconds", 1.0)
    await asyncio.sleep(duration)
    return {"status": "slept", "duration_seconds": duration}


@register_task("failing_task")
async def task_failing(payload: Dict[str, Any]) -> None:
    """Deliberately raises an exception to test error capture and retries."""
    error_message = payload.get("error_message", "Simulated computational failure")
    raise RuntimeError(error_message)
