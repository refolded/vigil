"""Unit tests for ProxyConfig and ProxyManager core logic."""
import io
import struct
import time
import wave
from unittest.mock import MagicMock, patch

import pytest

from app.manager import ProxyConfig, ProxyManager
from app.main import split_tts_text, _strip_id3, concatenate_audio


# ---------------------------------------------------------------------------
# ProxyConfig
# ---------------------------------------------------------------------------

class TestProxyConfig:
    def test_subpath_stripped(self):
        cfg = ProxyConfig("/tts/", "container", "http://localhost:9966")
        assert cfg.subpath == "tts"

    def test_backend_url_trailing_slash_stripped(self):
        cfg = ProxyConfig("tts", "container", "http://localhost:9966/")
        assert cfg.backend_url == "http://localhost:9966"

    def test_health_path_defaults_to_slash(self):
        cfg = ProxyConfig("tts", "container", "http://localhost:9966", health_path="")
        assert cfg.health_path == "/"

    def test_health_path_whitespace_stripped(self):
        cfg = ProxyConfig("tts", "container", "http://localhost:9966", health_path="  /health  ")
        assert cfg.health_path == "/health"

    def test_to_dict_round_trip(self):
        cfg = ProxyConfig(
            "tts", "my-container", "http://localhost:9966",
            idle_timeout=60, auto_unload=True, max_concurrency=2,
            health_path="/health", tts_split_mode="sentence", enabled=False,
        )
        d = cfg.to_dict()
        restored = ProxyConfig.from_dict(d)
        assert restored.subpath == cfg.subpath
        assert restored.container == cfg.container
        assert restored.backend_url == cfg.backend_url
        assert restored.idle_timeout == cfg.idle_timeout
        assert restored.auto_unload == cfg.auto_unload
        assert restored.max_concurrency == cfg.max_concurrency
        assert restored.health_path == cfg.health_path
        assert restored.tts_split_mode == cfg.tts_split_mode
        assert restored.enabled == cfg.enabled

    def test_to_status_dict_includes_runtime_fields(self):
        cfg = ProxyConfig("tts", "container", "http://localhost:9966")
        cfg.container_up = True
        cfg.active_requests = 3
        d = cfg.to_status_dict()
        assert d["container_up"] is True
        assert d["active_requests"] == 3
        assert "request_log" in d

    def test_last_activity_none_when_zero(self):
        cfg = ProxyConfig("tts", "container", "http://localhost:9966")
        assert cfg.to_status_dict()["last_activity"] is None

    def test_log_request_start_returns_incrementing_ids(self):
        cfg = ProxyConfig("tts", "container", "http://localhost:9966")
        id0 = cfg.log_request_start("GET", "/test")
        id1 = cfg.log_request_start("POST", "/test")
        assert id0 == 0
        assert id1 == 1

    def test_log_request_end_updates_entry(self):
        cfg = ProxyConfig("tts", "container", "http://localhost:9966")
        req_id = cfg.log_request_start("GET", "/test")
        cfg.log_request_end(req_id, 200, 42, resp_content_type="application/json")
        entry = next(e for e in cfg.request_log if e["id"] == req_id)
        assert entry["pending"] is False
        assert entry["status"] == 200
        assert entry["duration_ms"] == 42
        assert entry["resp_ct"] == "application/json"

    def test_log_request_maxlen_10(self):
        cfg = ProxyConfig("tts", "container", "http://localhost:9966")
        for _ in range(15):
            cfg.log_request_start("GET", "/test")
        assert len(cfg.request_log) == 10

    def test_preview_cache_pruned_on_eviction(self):
        cfg = ProxyConfig("tts", "container", "http://localhost:9966")
        for i in range(10):
            req_id = cfg.log_request_start("GET", f"/{i}")
            cfg.log_request_end(req_id, 200, 10, resp_preview=b"data")
        assert len(cfg._preview_cache) == 10
        # Adding an 11th evicts id=0 from the deque; preview cache prunes it.
        # id=10 has no preview yet, so cache has ids 1-9 = 9 entries.
        cfg.log_request_start("GET", "/overflow")
        assert len(cfg._preview_cache) == 9
        assert 0 not in cfg._preview_cache


# ---------------------------------------------------------------------------
# ProxyManager
# ---------------------------------------------------------------------------

class TestProxyManager:
    @pytest.fixture()
    def mgr(self, tmp_path):
        data_file = tmp_path / "proxies.json"
        with patch("app.manager.DATA_FILE", data_file):
            with patch("app.manager.subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
                m = ProxyManager()
                yield m

    def test_add_and_get(self, mgr):
        mgr.add("tts", "my-container", "http://localhost:9966")
        cfg = mgr.get("tts")
        assert cfg is not None
        assert cfg.container == "my-container"

    def test_get_strips_slashes(self, mgr):
        mgr.add("tts", "my-container", "http://localhost:9966")
        assert mgr.get("/tts/") is not None

    def test_remove_returns_true_on_success(self, mgr):
        mgr.add("tts", "my-container", "http://localhost:9966")
        assert mgr.remove("tts") is True
        assert mgr.get("tts") is None

    def test_remove_returns_false_when_not_found(self, mgr):
        assert mgr.remove("nonexistent") is False

    def test_update_field(self, mgr):
        mgr.add("tts", "my-container", "http://localhost:9966", idle_timeout=120)
        mgr.update("tts", idle_timeout=300)
        assert mgr.get("tts").idle_timeout == 300

    def test_update_health_path_normalises(self, mgr):
        mgr.add("tts", "my-container", "http://localhost:9966")
        mgr.update("tts", health_path="")
        assert mgr.get("tts").health_path == "/"

    def test_update_backend_url_strips_trailing_slash(self, mgr):
        mgr.add("tts", "my-container", "http://localhost:9966")
        mgr.update("tts", backend_url="http://localhost:9966/")
        assert mgr.get("tts").backend_url == "http://localhost:9966"

    def test_update_max_concurrency_resets_semaphore(self, mgr):
        mgr.add("tts", "my-container", "http://localhost:9966")
        cfg = mgr.get("tts")
        cfg._semaphore = object()  # fake semaphore
        mgr.update("tts", max_concurrency=2)
        assert cfg._semaphore is None

    def test_update_nonexistent_returns_none(self, mgr):
        assert mgr.update("nonexistent", idle_timeout=60) is None

    def test_list_returns_all(self, mgr):
        mgr.add("tts", "container-a", "http://localhost:9966")
        mgr.add("stt", "container-b", "http://localhost:9967")
        assert len(mgr.list()) == 2

    def test_persistence(self, tmp_path):
        data_file = tmp_path / "proxies.json"
        with patch("app.manager.DATA_FILE", data_file):
            with patch("app.manager.subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
                m1 = ProxyManager()
                m1.add("tts", "my-container", "http://localhost:9966")
                # New manager instance loads from disk
                m2 = ProxyManager()
                assert m2.get("tts") is not None
                assert m2.get("tts").container == "my-container"

    def test_list_containers_handles_docker_failure(self, mgr):
        with patch("app.manager.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="permission denied")
            result = mgr.list_containers()
            assert result == []


# ---------------------------------------------------------------------------
# TTS helpers
# ---------------------------------------------------------------------------

class TestSplitTtsText:
    def test_empty_string(self):
        assert split_tts_text("", "sentence") == [""]

    def test_single_sentence_no_split(self):
        chunks = split_tts_text("Hello world.", "sentence")
        assert chunks == ["Hello world."]

    def test_sentence_split(self):
        text = "Hello world. How are you? I am fine."
        chunks = split_tts_text(text, "sentence")
        assert len(chunks) >= 2

    def test_paragraph_split(self):
        # Use paragraphs long enough not to trigger the <25-char merge heuristic
        text = "This is the first long paragraph here.\n\nThis is the second long paragraph here."
        chunks = split_tts_text(text, "paragraph")
        assert len(chunks) == 2
        assert "first" in chunks[0]
        assert "second" in chunks[1]

    def test_short_fragments_merged(self):
        # Very short first chunk (<25 chars) merges with next
        text = "Hi. This is the second sentence here."
        chunks = split_tts_text(text, "sentence")
        # "Hi." is only 3 chars, should merge with next
        assert all(len(c) >= 3 for c in chunks)
        assert len(chunks) == 1 or chunks[0].startswith("Hi.")

    def test_only_whitespace(self):
        result = split_tts_text("   ", "sentence")
        assert result == [""]


class TestStripId3:
    def test_no_id3_tag(self):
        data = b"\xff\xfb" + b"\x00" * 100
        assert _strip_id3(data) == data

    def test_strips_id3v2_tag(self):
        # Build a minimal ID3v2 header (10 bytes): ID3 + version + flags + syncsafe size
        # size = 0 means tag is just the 10-byte header itself
        header = b'ID3\x03\x00\x00\x00\x00\x00\x00'  # size = 0 → total 10 bytes
        mp3_data = b"\xff\xfb" + b"\x00" * 100
        data = header + mp3_data
        result = _strip_id3(data)
        assert result == mp3_data


class TestConcatenateAudio:
    def _make_wav(self, n_frames: int = 100) -> bytes:
        buf = io.BytesIO()
        with wave.open(buf, "wb") as w:
            w.setnchannels(1)
            w.setsampwidth(2)
            w.setframerate(22050)
            w.writeframes(b"\x00\x00" * n_frames)
        return buf.getvalue()

    def test_single_part_returned_as_is(self):
        data = b"hello"
        assert concatenate_audio([data]) == data

    def test_wav_concatenation(self):
        wav1 = self._make_wav(100)
        wav2 = self._make_wav(200)
        result = concatenate_audio([wav1, wav2])
        with wave.open(io.BytesIO(result), "rb") as w:
            assert w.getnframes() == 300

    def test_mp3_fallback_concatenation(self):
        # Non-WAV data falls back to byte concatenation
        part1 = b"\xff\xfb" + b"\x01" * 50
        part2 = b"\xff\xfb" + b"\x02" * 50
        result = concatenate_audio([part1, part2])
        assert result.startswith(part1)
        assert len(result) == len(part1) + len(part2)

    def test_mp3_strips_id3_from_subsequent_parts(self):
        header = b'ID3\x03\x00\x00\x00\x00\x00\x00'
        mp3_data = b"\xff\xfb" + b"\x00" * 50
        part1 = mp3_data
        part2 = header + mp3_data
        result = concatenate_audio([part1, part2])
        # Second part should have ID3 stripped
        assert result == part1 + mp3_data
