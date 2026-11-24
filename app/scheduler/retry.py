import random
from typing import Tuple


def calculate_backoff_delay(
    retry_count: int,
    base_seconds: float = 1.0,
    max_seconds: float = 60.0,
    jitter: bool = True,
) -> float:
    """Calculates exponential backoff delay with decorrelated jitter.
    
    Formula:
      raw_delay = min(max_seconds, base_seconds * (2 ** retry_count))
      jittered_delay = (raw_delay / 2) + uniform(0, raw_delay / 2)
      
    Prevents thundering herd problems across concurrently failing jobs.
    """
    raw_delay = min(max_seconds, base_seconds * (2 ** max(0, retry_count)))
    if not jitter:
        return raw_delay

    half_delay = raw_delay / 2.0
    jittered = half_delay + random.uniform(0.0, half_delay)
    return round(jittered, 3)
