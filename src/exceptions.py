"""Cross-cutting domain exceptions shared across processors and the worker."""


class UnprocessableInputError(Exception):
    """Input data cannot be processed and must not be retried.

    Raised for deterministic, data-shape failures (the same input will always
    fail) rather than transient errors. The worker maps this to a ``SKIPPED``
    outcome — ack, no retry, no dead-letter — so the unit is visibly skipped on
    the dashboard with its reason instead of churning through retries/DLQ.
    """
