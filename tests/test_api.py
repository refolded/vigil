"""Integration tests for the Vigil FastAPI endpoints."""
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _add_proxy(client: TestClient, subpath="tts", **kwargs) -> dict:
    payload = {
        "subpath": subpath,
        "container": "my-container",
        "backend_url": "http://localhost:9966",
        **kwargs,
    }
    r = client.post("/api/proxies", json=payload)
    assert r.status_code == 201, r.text
    return r.json()


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class TestHealth:
    def test_health_ok(self, client):
        r = client.get("/api/health")
        assert r.status_code == 200
        assert r.json() == {"ok": True}


# ---------------------------------------------------------------------------
# Proxy CRUD
# ---------------------------------------------------------------------------

class TestProxyCRUD:
    def test_list_proxies_empty(self, client):
        r = client.get("/api/proxies")
        assert r.status_code == 200
        assert r.json() == []

    def test_add_proxy(self, client):
        data = _add_proxy(client, subpath="tts", idle_timeout=60)
        assert data["subpath"] == "tts"
        assert data["idle_timeout"] == 60

    def test_add_proxy_appears_in_list(self, client):
        _add_proxy(client)
        r = client.get("/api/proxies")
        assert r.status_code == 200
        assert len(r.json()) == 1
        assert r.json()[0]["subpath"] == "tts"

    def test_add_proxy_duplicate_returns_409(self, client):
        _add_proxy(client)
        r = client.post("/api/proxies", json={
            "subpath": "tts",
            "container": "other",
            "backend_url": "http://localhost:9967",
        })
        assert r.status_code == 409

    def test_add_proxy_empty_subpath_returns_400(self, client):
        r = client.post("/api/proxies", json={
            "subpath": "",
            "container": "my-container",
            "backend_url": "http://localhost:9966",
        })
        assert r.status_code == 400

    def test_add_proxy_reserved_subpath_returns_400(self, client):
        r = client.post("/api/proxies", json={
            "subpath": "api",
            "container": "my-container",
            "backend_url": "http://localhost:9966",
        })
        assert r.status_code == 400

    def test_add_proxy_strips_leading_slash(self, client):
        r = client.post("/api/proxies", json={
            "subpath": "/tts",
            "container": "my-container",
            "backend_url": "http://localhost:9966",
        })
        assert r.status_code == 201
        assert r.json()["subpath"] == "tts"

    def test_remove_proxy(self, client):
        _add_proxy(client)
        r = client.delete("/api/proxies/tts")
        assert r.status_code == 200
        assert r.json() == {"ok": True}
        assert client.get("/api/proxies").json() == []

    def test_remove_nonexistent_returns_404(self, client):
        r = client.delete("/api/proxies/nonexistent")
        assert r.status_code == 404

    def test_patch_proxy_idle_timeout(self, client):
        _add_proxy(client, idle_timeout=120)
        r = client.patch("/api/proxies/tts", json={"idle_timeout": 300})
        assert r.status_code == 200
        assert r.json()["idle_timeout"] == 300

    def test_patch_proxy_enabled_false(self, client):
        _add_proxy(client)
        r = client.patch("/api/proxies/tts", json={"enabled": False})
        assert r.status_code == 200
        assert r.json()["enabled"] is False

    def test_patch_nonexistent_returns_404(self, client):
        r = client.patch("/api/proxies/nonexistent", json={"idle_timeout": 60})
        assert r.status_code == 404

    def test_patch_health_path_normalised(self, client):
        _add_proxy(client)
        r = client.patch("/api/proxies/tts", json={"health_path": ""})
        assert r.status_code == 200
        assert r.json()["health_path"] == "/"


# ---------------------------------------------------------------------------
# Proxy actions
# ---------------------------------------------------------------------------

class TestProxyActions:
    def test_stop_proxy(self, client):
        _add_proxy(client)
        r = client.post("/api/proxies/tts/stop")
        assert r.status_code == 200
        assert r.json() == {"ok": True}

    def test_stop_nonexistent_returns_404(self, client):
        r = client.post("/api/proxies/nonexistent/stop")
        assert r.status_code == 404

    def test_restart_proxy(self, client, mock_health_check):
        _add_proxy(client)
        r = client.post("/api/proxies/tts/restart")
        assert r.status_code == 200
        assert r.json() == {"ok": True}

    def test_restart_nonexistent_returns_404(self, client):
        r = client.post("/api/proxies/nonexistent/restart")
        assert r.status_code == 404

    def test_restart_returns_502_when_container_fails(self, client, mock_docker):
        _add_proxy(client)
        # Make health check always fail so _wait_for_ready times out
        with patch("app.manager.ProxyManager._wait_for_ready", return_value=False):
            r = client.post("/api/proxies/tts/restart")
        assert r.status_code == 502

    def test_unload_stops_when_auto_unload_true(self, client):
        _add_proxy(client, auto_unload=True)
        r = client.post("/api/proxies/tts/unload")
        assert r.status_code == 200
        data = r.json()
        assert data["ok"] is True
        assert data["auto_unload"] is True

    def test_unload_nonexistent_returns_404(self, client):
        r = client.post("/api/proxies/nonexistent/unload")
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# Containers
# ---------------------------------------------------------------------------

class TestContainers:
    def test_list_containers(self, client):
        r = client.get("/api/containers")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)
        assert len(data) >= 1
        assert data[0]["Names"] == "test-container"

    def test_list_containers_docker_failure_returns_empty(self, client):
        with patch("app.manager.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error")
            r = client.get("/api/containers")
        assert r.status_code == 200
        assert r.json() == []


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

class TestUI:
    def test_root_returns_html(self, client):
        r = client.get("/")
        assert r.status_code == 200
        assert "text/html" in r.headers["content-type"]
        assert "Vigil" in r.text
