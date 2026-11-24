"""Worker entrypoint for `python -m app.worker`."""
import asyncio
import sys
from app.worker.main import run_worker

if __name__ == "__main__":
    try:
        asyncio.run(run_worker())
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)
