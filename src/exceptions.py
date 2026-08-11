"""Cross-cutting domain exceptions shared across processors and the worker."""


class UnprocessableInputError(Exception):
    """Input data cannot be processed and must not be retried.

    Raised for deterministic, data-shape failures (the same input will always
    fail) rather than transient errors. The worker maps this to a ``SKIPPED``
    outcome — ack, no retry, no dead-letter — so the unit is visibly skipped on
    the dashboard with its reason instead of churning through retries/DLQ.
    """


class ForecastNotAvailableError(Exception):
    """A forecast run is not published yet upstream (e.g. HTTP 403/404).

    The worker maps this to ``SKIPPED`` and acks without retrying: the producer
    re-emits the run on a later discovery tick, once it exists.
    """


class TransientDownloadError(Exception):
    """A download failed for a reason that may resolve on its own (5xx, timeout).

    The worker maps this to ``REQUEUED``: progress is released so the next
    discovery tick re-emits the unit, giving a natural availability-gated
    backoff instead of a tight retry loop against a throttled endpoint.
    """


class SourceFileNotFoundError(Exception):
    """The WorkUnit's source raw file does not exist (pruned/removed upstream).

    Raised when the download step cannot find the raw file (the feed/simulator
    pruned it before the worker got to it). The worker maps this to a terminal,
    NON-retryable ``ERROR`` outcome — ack, no retry, no dead-letter — so the
    missing file surfaces as a visible failure on the dashboard (fail rate =
    error + dlq) instead of churning through re-downloads that can never succeed.
    """
