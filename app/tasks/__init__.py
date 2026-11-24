from app.tasks.registry import (
    register_task,
    get_task,
    list_registered_tasks,
    execute_task,
)
import app.tasks.ml_workloads  # Ensure all tasks are registered upon import

__all__ = [
    "register_task",
    "get_task",
    "list_registered_tasks",
    "execute_task",
]
