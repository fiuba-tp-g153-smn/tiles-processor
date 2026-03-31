# InlineProcessor lives in worker.inline_processor to keep the processors
# package free of main-process imports (memory isolation).
from worker.inline_processor import InlineProcessor  # noqa: F401

__all__ = ["InlineProcessor"]
