import collections
import json
import os
import subprocess
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional

import httpx

DATA_FILE = Path(os.environ.get("DATA_FILE", "/data/proxies.json"))
STARTUP_TIMEOUT = int(os.environ.get("STARTUP_TIMEOUT", "120"))


class ProxyConfig:
    def __init__(
        self,
        subpath: str,
        container: str,
        backend_url: str,
        idle_timeout: int = 120,
        auto_unload: bool = False,
    ):
        self.subpath = subpath.strip("/")
        self.container = container
        self.backend_url = backend_url.rstrip("/")
        self.idle_timeout = idle_timeout
        self.auto_unload = auto_unload
        # runtime state — not persisted
        self.last_activity: float = 0.0  # 0 = never used; idle watcher ignores until first request
        self.active_requests: int = 0
        self.container_up: Optional[bool] = None  # None = unknown
        self.request_log: collections.deque = collections.deque(maxlen=10)
        self._lock = threading.Lock()

    def to_dict(self) -> dict:
        return {
            "subpath": self.subpath,
            "container": self.container,
            "backend_url": self.backend_url,
            "idle_timeout": self.idle_timeout,
            "auto_unload": self.auto_unload,
        }

    def to_status_dict(self) -> dict:
        return {
            **self.to_dict(),
            "container_up": self.container_up,
            "active_requests": self.active_requests,
            "last_activity": self.last_activity if self.last_activity != 0.0 else None,
            "request_log": list(self.request_log),
        }

    def log_request(
        self,
        method: str,
        path: str,
        status: int,
        duration_ms: int,
        req_content_type: str = "",
        req_body: Optional[str] = None,
        resp_content_type: str = "",
        resp_body: Optional[str] = None,
        resp_size: int = 0,
    ):
        self.request_log.append({
            "method": method,
            "path": path,
            "status": status,
            "duration_ms": duration_ms,
            "ts": time.time(),
            "req_ct": req_content_type,
            "req_body": req_body,
            "resp_ct": resp_content_type,
            "resp_body": resp_body,
            "resp_size": resp_size,
        })

    @classmethod
    def from_dict(cls, d: dict) -> "ProxyConfig":
        return cls(
            subpath=d["subpath"],
            container=d["container"],
            backend_url=d["backend_url"],
            idle_timeout=d.get("idle_timeout", 120),
            auto_unload=d.get("auto_unload", False),
        )


class ProxyManager:
    def __init__(self):
        self._proxies: Dict[str, ProxyConfig] = {}
        self._lock = threading.Lock()
        self._on_change: Optional[callable] = None
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
            print(f"[manager] Loaded {len(self._proxies)} proxy config(s) from {DATA_FILE}", flush=True)
        except Exception as e:
            print(f"[manager] Failed to load {DATA_FILE}: {e}", flush=True)

    def _save(self):
        try:
            DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
            with self._lock:
                data = [p.to_dict() for p in self._proxies.values()]
            DATA_FILE.write_text(json.dumps(data, indent=2))
        except Exception as e:
            print(f"[manager] Failed to save {DATA_FILE}: {e}", flush=True)

    # ------------------------------------------------------------------
    # Proxy CRUD
    # ------------------------------------------------------------------

    def update(self, subpath: str, **kwargs) -> Optional[ProxyConfig]:
        cfg = self.get(subpath)
        if cfg is None:
            return None
        for k, v in kwargs.items():
            if hasattr(cfg, k):
                setattr(cfg, k, v)
        self._save()
        return cfg

    def add(self, subpath: str, container: str, backend_url: str, idle_timeout: int = 120, auto_unload: bool = True) -> ProxyConfig:
        cfg = ProxyConfig(subpath, container, backend_url, idle_timeout, auto_unload)
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
        while time.time() < deadline:
            try:
                with httpx.Client(timeout=3) as client:
                    client.get(cfg.backend_url + "/")
                return True
            except Exception:
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
            print(f"[manager] Starting {cfg.container!r}…", flush=True)
            subprocess.run(["docker", "start", cfg.container], capture_output=True)
            if not self._wait_for_ready(cfg):
                raise RuntimeError(
                    f"Container {cfg.container!r} did not become ready within {STARTUP_TIMEOUT}s"
                )
            cfg.container_up = True
            print(f"[manager] {cfg.container!r} ready.", flush=True)
            self._notify()

    def stop(self, cfg: ProxyConfig):
        """Stop the container, freeing VRAM. Next proxy request will cold-start it."""
        with cfg._lock:
            print(f"[manager] Stopping {cfg.container!r}…", flush=True)
            subprocess.run(["docker", "stop", cfg.container], capture_output=True)
            cfg.container_up = False
            print(f"[manager] {cfg.container!r} stopped.", flush=True)
        self._notify()

    def restart(self, cfg: ProxyConfig):
        """Stop then immediately start the container (model reloads, VRAM stays)."""
        with cfg._lock:
            print(f"[manager] Restarting {cfg.container!r}…", flush=True)
            subprocess.run(["docker", "stop", cfg.container], capture_output=True)
            cfg.container_up = False
            subprocess.run(["docker", "start", cfg.container], capture_output=True)
            if not self._wait_for_ready(cfg):
                raise RuntimeError(f"Container {cfg.container!r} did not become ready within {STARTUP_TIMEOUT}s")
            cfg.container_up = True
            cfg.last_activity = time.time()
            print(f"[manager] {cfg.container!r} restarted.", flush=True)
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
                # Sync container_up from reality — catches external stops/crashes
                if cfg.active_requests == 0:
                    actual = self._is_running(cfg.container)
                    if cfg.container_up != actual:
                        print(f"[manager] {cfg.container!r} state drift: {cfg.container_up!r} → {actual!r}", flush=True)
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
                        print(f"[manager] {cfg.container!r} idle {idle:.0f}s — stopping (auto-unload)", flush=True)
                        subprocess.run(["docker", "stop", cfg.container], capture_output=True)
                        cfg.container_up = False
                        self._notify()
                    else:
                        print(f"[manager] {cfg.container!r} idle {idle:.0f}s — restarting", flush=True)
                        subprocess.run(["docker", "stop", cfg.container], capture_output=True)
                        cfg.container_up = False
                        self._notify()
                        subprocess.run(["docker", "start", cfg.container], capture_output=True)
                        self._wait_for_ready(cfg)
                        cfg.container_up = True
                        cfg.last_activity = time.time()
                        self._notify()
