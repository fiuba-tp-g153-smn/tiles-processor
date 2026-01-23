import socket
import time
import requests
import pytest
from health_server import HealthCheckServer


def get_free_port():
    """Get a free port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


class TestHealthServer:

    @pytest.fixture
    def port(self):
        return get_free_port()

    def test_health_server_healthy(self, port):
        """Test health server when check returns True."""

        def check_healthy():
            return True, "All good"

        server = HealthCheckServer(port=port, check_readiness=check_healthy)
        server.start()

        # Give server time to start
        time.sleep(0.1)

        try:
            resp = requests.get(f"http://localhost:{port}/health")
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "ok"
            assert data["details"] == "All good"

        finally:
            server.stop()

    def test_health_server_unhealthy(self, port):
        """Test health server when check returns False."""

        def check_unhealthy():
            return False, "Something broke"

        server = HealthCheckServer(port=port, check_readiness=check_unhealthy)
        server.start()

        time.sleep(0.1)

        try:
            resp = requests.get(f"http://localhost:{port}/health")
            assert resp.status_code == 503
            data = resp.json()
            assert data["status"] == "error"
            assert data["details"] == "Something broke"

        finally:
            server.stop()

    def test_health_server_exception(self, port):
        """Test health server when check raises exception."""

        def check_exception():
            raise RuntimeError("Boom")

        server = HealthCheckServer(port=port, check_readiness=check_exception)
        server.start()

        time.sleep(0.1)

        try:
            resp = requests.get(f"http://localhost:{port}/health")
            assert resp.status_code == 503
            data = resp.json()
            assert data["status"] == "error"
            assert "Exception" in data["details"]

        finally:
            server.stop()

    def test_health_server_no_callback(self, port):
        """Test health server with no callback configured (default healthy)."""
        server = HealthCheckServer(port=port, check_readiness=None)
        server.start()

        time.sleep(0.1)

        try:
            resp = requests.get(f"http://localhost:{port}/health")
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "ok"

        finally:
            server.stop()

    def test_health_server_unknown_path(self, port):
        """Test 404 for unknown paths."""
        server = HealthCheckServer(port=port)
        server.start()

        time.sleep(0.1)

        try:
            resp = requests.get(f"http://localhost:{port}/invalid")
            assert resp.status_code == 404

        finally:
            server.stop()
