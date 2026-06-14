"""Exit codes for the processing subprocess, shared with the parent worker.

Processing runs in a separate process for memory isolation, so a typed
exception cannot cross the boundary — the process exit code is the signal. These
constants live in one module so the subprocess and the parent never drift.
"""

EXIT_SUCCESS_CODE = 0
EXIT_ERROR_CODE = 1
# Input is deterministically unprocessable: the parent maps this to a SKIPPED
# outcome (ack, no retry, no DLQ) instead of treating it as a failure.
EXIT_SKIP_CODE = 2

# Normal subprocess logs go to stdout; stderr is reserved for real errors. The
# subprocess prints the human-readable skip reason to stderr on a line with this
# prefix so the parent can extract it verbatim and surface it as the SKIPPED
# reason on the dashboard.
SKIP_REASON_PREFIX = "[UNPROCESSABLE] "
