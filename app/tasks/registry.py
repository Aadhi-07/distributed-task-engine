import asyncio
import inspect
from typing import Any, Callable, Dict, Optional
from app.core.logging import get_logger

logger = get_logger(__name__)

TaskFunction = Callable[..., Any]

_TASK_REGISTRY: Dict[str, TaskFunction] = {}


def register_task(name: str) -> Callable[[TaskFunction], TaskFunction]:
    """Decorator to register a callable function as an executable task type."""
    def decorator(fn: TaskFunction) -> TaskFunction:
        if name in _TASK_REGISTRY:
            logger.warning(f"Task type '{name}' is being overwritten in registry.")
        _TASK_REGISTRY[name] = fn
        return fn
    return decorator


def get_task(name: str) -> Optional[TaskFunction]:
    """Retrieves a registered task function by name."""
    return _TASK_REGISTRY.get(name)


def list_registered_tasks() -> list[str]:
    """Returns a list of all registered task types."""
    return list(_TASK_REGISTRY.keys())


async def execute_task(name: str, payload: Dict[str, Any]) -> Any:
    """Invokes the task handler, supporting both async and sync callable functions."""
    handler = get_task(name)
    if not handler:
        raise ValueError(f"Unknown task type '{name}'. Registered: {list_registered_tasks()}")

    # Call handler with payload as kwargs or as a single dict
    sig = inspect.signature(handler)
    if len(sig.parameters) == 1 and next(iter(sig.parameters.values())).kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
        # Pass payload as dict directly if function accepts single arg
        res = handler(payload)
    else:
        # Pass unpacked kwargs matching signature
        res = handler(**payload)

    if inspect.isawaitable(res):
        return await res
    return res
