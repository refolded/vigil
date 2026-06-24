import collections
import json
import logging
import subprocess
import threading
import time
from typing import Callable, Dict, List, Optional

import httpx

from .config import DATA_FILE, STARTUP_TIMEOUT

log = logging.getLogger(__name__)


class ProxyConfig:
    def __init__(
        self,
        subpath: str,
        container: str,
        backend_url: str,
        idle_timeout: int = 120,
        auto_unload: bool = False,
        max_concurrency: int = 0,
        health_path: str = "/",
        tts_split_mode: str = "none",
        enabled: bool = True,
    ):
        self.subpath = subpath.strip("/")
        self.container = container
        self.backend_url = backend_url.rstrip("/")
        self.idle_timeout = idle_timeout
        self.auto_unload = auto_unload
        self.max_concurrency = max_concurrency
        self.health_path = health_path.strip() or "/"
        self.tts_split_mode = tts_split_mode
        self.enabled = enabled
        # runtime state — not persisted
        self.last_activity: float = 0.0
        self.active_requests: int = 0
        self.queued_requests: int = 0
        self.container_up: Optional[bool] = None
        self._semaphore = None  # asyncio.Semaphore, lazy-init in async context
        self.request_log: collections.deque = collections.deque(maxlen=10)
        self._lock = threading.Lock()
        self._req_counter: int = 0
        self._preview_cache: dict = {}

    def to_dict(self) -> dict:
        return {
            "subpath": self.subpath,
            "container": self.container,
            "backend_url": self.backend_url,
            "idle_timeout": self.idle_timeout,
            "auto_unload": self.auto_unload,
            "max_concurrency": self.max_concurrency,
            "health_path": self.health_path,
            "tts_split_mode": self.tts_split_mode,
            "enabled": self.enabled,
        }

    def to_status_dict(self) -> dict:
        return {
            **self.to_dict(),
            "container_up": self.container_up,
            "active_requests": self.active_requests,
            "queued_requests": self.queued_requests,
            "last_activity": self.last_activity if self.last_activity != 0.0 else None,
            "request_log": list(self.request_log),
        }

    def log_request_start(
        self,
        method: str,
        path: str,
        client_ip: str = "",
        req_content_type: str = "",
        req_body: Optional[str] = None,
        req_headers: Optional[dict] = None,
    ) -> int:
        req_id = self._req_counter
        self._req_counter += 1
        self.request_log.append({
            "id": req_id,
            "method": method,
            "path": path,
            "ts": time.time(),
            "client_ip": client_ip,
            "pending": True,
            "req_ct": req_content_type,
            "req_headers": req_headers or {},
            "req_body": req_body,
            "status": None,
            "duration_ms": None,
            "resp_ct": "",
            "resp_headers": {},
            "resp_body": None,
            "resp_size": 0,
            "resp_is_media": False,
            "resp_has_preview": False,
        })
        active_ids = {e.get("id") for e in self.request_log}
        self._preview_cache = {k: v for k, v in self._preview_cache.items() if k in active_ids}
        return req_id

    def log_request_end(
        self,
        req_id: int,
        status: int,
        duration_ms: int,
        resp_content_type: str = "",
        resp_body: Optional[str] = None,
        resp_size: int = 0,
        resp_is_media: bool = False,
        resp_headers: Optional[dict] = None,
        resp_preview: Optional[bytes] = None,
    ):
        for entry in self.request_log:
            if entry.get("id") == req_id:
                entry.update({
                    "pending": False,
                    "status": status,
                    "duration_ms": duration_ms,
                    "resp_ct": resp_content_type,
                    "resp_headers": resp_headers or {},
                    "resp_body": resp_body,
                    "resp_size": resp_size,
                    "resp_is_media": resp_is_media,
                    "resp_has_preview": bool(resp_preview),
                })
                break
        if resp_preview is not None:
            self._preview_cache[req_id] = resp_preview

    @classmethod
    def from_dict(cls, d: dict) -> "ProxyConfig":
        return cls(
            subpath=d["subpath"],
            container=d["container"],
            backend_url=d["backend_url"],
            idle_timeout=d.get("idle_timeout", 120),
            auto_unload=d.get("auto_unload", False),
            max_concurrency=d.get("max_concurrency", 0),
            health_path=d.get("health_path", "/"),
            tts_split_mode=d.get("tts_split_mode", "none"),
            enabled=d.get("enabled", True),
        )


class ProxyManager:
    def __init__(self):
        self._proxies: Dict[str, ProxyConfig] = {}
        self._lock = threading.Lock()
        self._on_change: Optional[Callable[[], None]] = None
        self._load()
        threading.Thread(target=self._idle_watcher, daemon=True).start()

    def _notify(self):
        if self._on_change:
            try:
                self._on_change()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load(self):
        if not DATA_FILE.exists():
            return
        try:
            data = json.loads(DATA_FILE.read_text())
            for d in data:
                cfg = ProxyConfig.from_dict(d)
                self._proxies[cfg.subpath] = cfg
            log.info("Loaded %d proxy config(s) from %s", len(self._proxies), DATA_FILE)
        except Exception as e:
            log.error("Failed to load %s: %s", DATA_FILE, e)

    def _save(self):
        try:
            DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
            with self._lock:
                data = [p.to_dict() for p in self._proxies.values()]
            DATA_FILE.write_text(json.dumps(data, indent=2))
        except Exception as e:
            log.error("Failed to save %s: %s", DATA_FILE, e)

    # ------------------------------------------------------------------
    # Proxy CRUD
    # ------------------------------------------------------------------

    def update(self, subpath: str, **kwargs) -> Optional[ProxyConfig]:
        cfg = self.get(subpath)
        if cfg is None:
            return None
        # Apply field normalization for fields that require it
        if "health_path" in kwargs:
            kwargs["health_path"] = (kwargs["health_path"] or "").strip() or "/"
        if "backend_url" in kwargs:
            kwargs["backend_url"] = kwargs["backend_url"].rstrip("/")
        for k, v in kwargs.items():
            if hasattr(cfg, k):
                setattr(cfg, k, v)
        # Reset semaphore so it's re-created with the new limit next request
        if "max_concurrency" in kwargs:
            cfg._semaphore = None
        self._save()
        return cfg

    def add(
        self,
        subpath: str,
        container: str,
        backend_url: str,
        idle_timeout: int = 120,
        auto_unload: bool = False,
        max_concurrency: int = 0,
        health_path: str = "/",
        tts_split_mode: str = "none",
        enabled: bool = True,
    ) -> ProxyConfig:
        cfg = ProxyConfig(
            subpath, container, backend_url, idle_timeout, auto_unload,
            max_concurrency, health_path, tts_split_mode, enabled,
        )
        with self._lock:
            self._proxies[cfg.subpath] = cfg
        self._save()
        return cfg

    def remove(self, subpath: str) -> bool:
        subpath = subpath.strip("/")
        with self._lock:
            cfg = self._proxies.pop(subpath, None)
        if cfg:
            self._save()
        return cfg is not None

    def get(self, subpath: str) -> Optional[ProxyConfig]:
        return self._proxies.get(subpath.strip("/"))

    def list(self) -> List[ProxyConfig]:
        with self._lock:
            return list(self._proxies.values())

    # ------------------------------------------------------------------
    # Docker helpers
    # ------------------------------------------------------------------

    def list_containers(self) -> list:
        r = subprocess.run(
            ["docker", "ps", "-a", "--format", "{{json .}}"],
            capture_output=True,
            text=True,
        )
        if r.returncode != 0:
            log.warning("docker ps failed (rc=%d): %s", r.returncode, r.stderr.strip())
            return []
        result = []
        for line in r.stdout.strip().splitlines():
            line = line.strip()
            if line:
                try:
                    result.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
        return result

    def _is_running(self, name: str) -> bool:
        r = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Status}}", name],
            capture_output=True,
            text=True,
        )
        return r.stdout.strip() == "running"

    def _wait_for_ready(self, cfg: ProxyConfig) -> bool:
        deadline = time.time() + STARTUP_TIMEOUT
        url = cfg.backend_url + cfg.health_path
        while time.time() < deadline:
            try:
                with httpx.Client(timeout=3) as client:
                    client.get(url)
                return True  # any HTTP response = server is listening
            except Exception:
                pass
            time.sleep(2)
        return False

    def ensure_running(self, cfg: ProxyConfig):
        """Start container if not running. Blocking — call via run_in_executor."""
        if cfg.container_up is True:
            return
        with cfg._lock:
            if cfg.container_up is True:
                return
            if self._is_running(cfg.container):
                cfg.container_up = True
                return
            log.info("Starting %r…", cfg.container)
            subprocess.run(["docker", "start", cfg.container], capture_output=True)
            if not self._wait_for_ready(cfg):
                raise RuntimeError(
                    f"Container {cfg.container!r} did not become ready within {STARTUP_TIMEOUT}s"
                )
            cfg.container_up = True
            log.info("%r ready.", cfg.container)
            self._notify()

    def stop(self, cfg: ProxyConfig):
        """Stop the container, freeing VRAM. Next proxy request will cold-start it."""
        with cfg._lock:
            log.info("Stopping %r…", cfg.container)
            subprocess.run(["docker", "stop", cfg.container], capture_output=True)
            cfg.container_up = False
            log.info("%r stopped.", cfg.container)
        self._notify()

    def restart(self, cfg: ProxyConfig):
        """Stop then immediately start the container (model reloads, VRAM stays)."""
        with cfg._lock:
            log.info("Restarting %r…", cfg.container)
            subprocess.run(["docker", "stop", cfg.container], capture_output=True)
            cfg.container_up = False
            subprocess.run(["docker", "start", cfg.container], capture_output=True)
            if not self._wait_for_ready(cfg):
                raise RuntimeError(
                    f"Container {cfg.container!r} did not become ready within {STARTUP_TIMEOUT}s"
                )
            cfg.container_up = True
            cfg.last_activity = time.time()
            log.info("%r restarted.", cfg.container)
        self._notify()

    def unload(self, cfg: ProxyConfig):
        """Respects auto_unload: stop-only if on, restart if off."""
        if cfg.auto_unload:
            self.stop(cfg)
        else:
            self.restart(cfg)

    # ------------------------------------------------------------------
    # Idle watcher
    # ------------------------------------------------------------------

    def _idle_watcher(self):
        while True:
            time.sleep(20)
            for cfg in self.list():
                if not cfg.enabled:
                    continue
                # Sync container_up from reality — catches external stops/crashes
                if cfg.active_requests == 0:
                    actual = self._is_running(cfg.container)
                    if cfg.container_up != actual:
                        log.info(
                            "%r state drift: %r → %r",
                            cfg.container, cfg.container_up, actual,
                        )
                        cfg.container_up = actual
                        self._notify()

                if not cfg.container_up or cfg.last_activity == 0.0:
                    continue
                with cfg._lock:
                    if cfg.active_requests > 0:
                        continue
                    idle = time.time() - cfg.last_activity
                    if idle < cfg.idle_timeout:
                        continue

                if cfg.auto_unload:
                    log.info("%r idle %.0fs — stopping (auto-unload)", cfg.container, idle)
                    self.stop(cfg)
                else:
                    log.info("%r idle %.0fs — restarting", cfg.container, idle)
                    try:
                        self.restart(cfg)
                    except RuntimeError as e:
                        log.error("Idle restart failed for %r: %s", cfg.container, e)
