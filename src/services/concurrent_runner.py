"""Shared async concurrency helper for bounded parallel processing."""

import asyncio
import logging
from typing import Callable, TypeVar, cast

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


async def run_concurrently(
    items: dict[str, T],
    worker_fn: Callable[[str, T], R],
    max_concurrency: int = 4,
    task_name: str = "Task",
) -> dict[str, R]:
    """Run a sync worker function over dict items with bounded concurrency.

    Uses Semaphore + asyncio.to_thread + gather to process items in parallel
    while limiting concurrent executions to control memory usage.

    Args:
        items: Mapping of names to input values.
        worker_fn: Sync function called as worker_fn(name, value) in a thread.
        max_concurrency: Maximum simultaneous executions.
        task_name: Label for error messages.

    Returns:
        Dict mapping names to results for successful items.

    Raises:
        RuntimeError: If any item fails, after logging all failures.
    """
    semaphore = asyncio.Semaphore(max_concurrency)

    async def bounded_work(name: str, value: T) -> R:
        async with semaphore:
            return await asyncio.to_thread(worker_fn, name, value)

    names = list(items.keys())
    tasks = [bounded_work(name, items[name]) for name in names]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    successful: dict[str, R] = {}
    failed = []

    for name, result in zip(names, results):
        if isinstance(result, Exception):
            failed.append((name, result))
        else:
            successful[name] = cast(R, result)

    if failed:
        for name, err in failed:
            logger.error("%s failed for %s: %s", task_name, name, err)
        raise RuntimeError(f"{task_name} failed for {len(failed)}/{len(tasks)} files")

    return successful
