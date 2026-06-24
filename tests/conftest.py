"""Shared fixtures for Vigil tests."""
import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def data_file(tmp_path):
    """Temporary data file path for proxy config persistence."""
    return tmp_path / "proxies.json"


@pytest.fixture()
def mock_docker():
    """Patch subprocess.run to simulate Docker CLI responses."""
    with patch("app.manager.subprocess.run") as mock_run:
        # Default: all docker commands succeed, containers are running
        def _docker(cmd, **kwargs):
            result = MagicMock()
            result.returncode = 0
            result.stdout = ""
            result.stderr = ""
            if "inspect" in cmd and "--format" in cmd:
                result.stdout = "running\n"
            elif "ps" in cmd:
                result.stdout = json.dumps({
                    "ID": "abc123",
                    "Names": "test-container",
                    "Image": "test/image:latest",
                    "State": "running",
                    "Status": "Up 5 minutes",
                    "Ports": "0.0.0.0:9966->9966/tcp",
                }) + "\n"
            return result

        mock_run.side_effect = _docker
        yield mock_run


@pytest.fixture()
def mock_health_check():
    """Patch httpx health check to return immediately (any response = ready)."""
    with patch("app.manager.httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = MagicMock()  # any response = server listening
        mock_client_cls.return_value = mock_client
        yield mock_client


@pytest.fixture()
def client(data_file, mock_docker, mock_health_check):
    """TestClient with isolated data file and mocked Docker."""
    with patch("app.config.DATA_FILE", data_file):
        with patch("app.manager.DATA_FILE", data_file):
            # Import after patching so the manager sees the patched DATA_FILE
            from app.main import app, manager
            # Reset manager state between tests
            manager._proxies.clear()
            with TestClient(app, raise_server_exceptions=True) as c:
                yield c
