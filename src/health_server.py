"""HTTP health check server for container liveness and readiness probes."""

import json
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Callable, cast

logger = logging.getLogger(__name__)


class HealthRequestHandler(BaseHTTPRequestHandler):
    """
    Request handler for health checks.

    Endpoint:
        GET /health: returns 200 when the process is up and its registered
            readiness check passes, else 503 (with no callback registered, a
            bare 200 — liveness only). This is the only route; the container
            HEALTHCHECK and startup gating (``depends_on: service_healthy``)
            probe it. There is deliberately no separate ``/ready`` route — no
            probe uses one anywhere in the stack.
    """

    def do_GET(self):  # pylint: disable=invalid-name
        """Handle GET requests for the health check endpoint."""
        if self.path == "/health":
            self._handle_health()
        else:
            self.send_error(404, "Not Found")

    def _handle_health(self):
        """Health check - verifies process is running and dependencies are healthy."""
        server = cast("HealthServer", self.server)
        if not server.check_readiness_callback:
            # If no callback registered, assume healthy
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "ok", "checks": "none"}')
            return

        try:
            is_healthy, details = server.check_readiness_callback()

            if is_healthy:
                self.send_response(200)
            else:
                self.send_response(503)

            self.send_header("Content-type", "application/json")
            self.end_headers()

            response = json.dumps(
                {"status": "ok" if is_healthy else "error", "details": details}
            ).encode("utf-8")

            self.wfile.write(response)

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("Error checking health: %s", e)
            self.send_response(503)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(
                b'{"status": "error", "details": "Exception during check"}'
            )

    def log_message(
        self, format, *args  # pylint: disable=redefined-builtin
    ):  # pylint: disable=arguments-differ
        """Suppress default HTTP logging to stdout to keep logs clean."""


class HealthServer(HTTPServer):
    """Extended HTTPServer to store the readiness callback."""

    def __init__(
        self, server_address, RequestHandlerClass, check_readiness_callback=None
    ):
        super().__init__(server_address, RequestHandlerClass)
        self.check_readiness_callback = check_readiness_callback


class HealthCheckServer:
    """Manages the health check HTTP server in a separate thread."""

    def __init__(self, port: int = 8080, check_readiness: Callable | None = None):
        self._port = port
        self._check_readiness = check_readiness
        self._server: HealthServer | None = None
        self._thread: threading.Thread | None = None

    def start(self):
        """Start the health server in a daemon thread."""
        try:
            self._server = HealthServer(
                ("", self._port),
                HealthRequestHandler,
                check_readiness_callback=self._check_readiness,
            )

            self._thread = threading.Thread(target=self._server.serve_forever)
            self._thread.daemon = True
            self._thread.start()

            logger.info("Health check server started on port %d", self._port)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("Failed to start health check server: %s", e)

    def stop(self):
        """Stop the health server."""
        if self._server:
            self._server.shutdown()
            self._server.server_close()
            logger.info("Health check server stopped")
