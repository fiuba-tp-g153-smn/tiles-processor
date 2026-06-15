"""Cross-cutting domain exceptions shared across processors and the worker."""


class UnprocessableInputError(Exception):
    """Input data cannot be processed and must not be retried.

    Raised for deterministic, data-shape failures (the same input will always
    fail) rather than transient errors. The worker maps this to a ``SKIPPED``
    outcome — ack, no retry, no dead-letter — so the unit is visibly skipped on
    the dashboard with its reason instead of churning through retries/DLQ.
    """


class SourceFileNotFoundError(Exception):
    """The WorkUnit's source raw file does not exist (pruned/removed upstream).

    Raised when the download step cannot find the raw file (the feed/simulator
    pruned it before the worker got to it). The worker maps this to a terminal,
    NON-retryable ``ERROR`` outcome — ack, no retry, no dead-letter — so the
    missing file surfaces as a visible failure on the dashboard (fail rate =
    error + dlq) instead of churning through re-downloads that can never succeed.
    """
