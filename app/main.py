import asyncio
import json
import time
from pathlib import Path
from typing import Optional, Set

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, StreamingResponse
from starlette.requests import ClientDisconnect
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .manager import ProxyManager

STATIC_DIR = Path(__file__).parent.parent / "static"

manager = ProxyManager()
app = FastAPI(title="Docker Idle Proxy", docs_url="/api/docs")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# ---------------------------------------------------------------------------
# SSE broadcast
# ---------------------------------------------------------------------------

_sse_queues: Set[asyncio.Queue] = set()
_main_loop: Optional[asyncio.AbstractEventLoop] = None


@app.on_event("startup")
async def _startup():
    global _main_loop
    _main_loop = asyncio.get_running_loop()
    manager._on_change = _schedule_broadcast


def _schedule_broadcast():
    """Call from any thread to push a proxy-list update to all SSE clients."""
    if _main_loop and _main_loop.is_running():
        asyncio.run_coroutine_threadsafe(_broadcast(), _main_loop)


async def _broadcast():
    if not _sse_queues:
        return
    payload = "data: " + json.dumps([p.to_status_dict() for p in manager.list()]) + "\n\n"
    dead: Set[asyncio.Queue] = set()
    for q in list(_sse_queues):
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            dead.add(q)
    _sse_queues.difference_update(dead)


@app.get("/api/events", include_in_schema=False)
async def sse_events(request: Request):
    q: asyncio.Queue = asyncio.Queue(maxsize=20)
    _sse_queues.add(q)

    async def stream():
        try:
            # Send current state immediately on connect
            yield "data: " + json.dumps([p.to_status_dict() for p in manager.list()]) + "\n\n"
            while True:
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=25)
                    yield msg
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            _sse_queues.discard(q)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )

HOP_BY_HOP = frozenset([
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade",
])

RESERVED = {"api"}

MAX_BODY_CAPTURE = 8 * 1024  # 8 KB per request log entry


def _is_text_ct(ct: str) -> bool:
    ct = ct.lower()
    return any(t in ct for t in ("json", "text/", "xml", "html", "yaml"))

# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def ui():
    return (STATIC_DIR / "index.html").read_text()

# ---------------------------------------------------------------------------
# API — containers
# ---------------------------------------------------------------------------

@app.get("/api/containers", tags=["containers"])
async def list_containers():
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, manager.list_containers)

# ---------------------------------------------------------------------------
# API — proxies
# ---------------------------------------------------------------------------

class ProxyBody(BaseModel):
    subpath: str
    container: str
    backend_url: str
    idle_timeout: int = 120
    auto_unload: bool = False


class ProxyPatch(BaseModel):
    idle_timeout: Optional[int] = None
    auto_unload: Optional[bool] = None


@app.get("/api/proxies", tags=["proxies"])
async def list_proxies():
    return [p.to_status_dict() for p in manager.list()]


@app.post("/api/proxies", status_code=201, tags=["proxies"])
async def add_proxy(body: ProxyBody):
    subpath = body.subpath.strip("/")
    if not subpath:
        raise HTTPException(400, "subpath cannot be empty")
    if subpath in RESERVED:
        raise HTTPException(400, f"'{subpath}' is reserved")
    if manager.get(subpath):
        raise HTTPException(409, f"/{subpath} already configured")
    cfg = manager.add(subpath, body.container, body.backend_url, body.idle_timeout, body.auto_unload)
    asyncio.create_task(_broadcast())
    return cfg.to_dict()


@app.patch("/api/proxies/{subpath:path}", tags=["proxies"])
async def patch_proxy(subpath: str, body: ProxyPatch):
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    cfg = manager.update(subpath, **updates)
    if cfg is None:
        raise HTTPException(404, f"No proxy for /{subpath}")
    asyncio.create_task(_broadcast())
    return cfg.to_status_dict()


@app.delete("/api/proxies/{subpath:path}", tags=["proxies"])
async def remove_proxy(subpath: str):
    if not manager.remove(subpath):
        raise HTTPException(404, f"No proxy for /{subpath}")
    asyncio.create_task(_broadcast())
    return {"ok": True}


@app.post("/api/proxies/{subpath:path}/stop", tags=["proxies"])
async def stop_proxy(subpath: str):
    cfg = manager.get(subpath)
    if cfg is None:
        raise HTTPException(404, f"No proxy for /{subpath}")
    await asyncio.get_running_loop().run_in_executor(None, manager.stop, cfg)
    asyncio.create_task(_broadcast())
    return {"ok": True}


@app.post("/api/proxies/{subpath:path}/unload", tags=["proxies"])
async def unload_proxy(subpath: str):
    cfg = manager.get(subpath)
    if cfg is None:
        raise HTTPException(404, f"No proxy for /{subpath}")
    try:
        await asyncio.get_running_loop().run_in_executor(None, manager.unload, cfg)
    except RuntimeError as e:
        raise HTTPException(502, str(e))
    asyncio.create_task(_broadcast())
    return {"ok": True, "auto_unload": cfg.auto_unload}

# ---------------------------------------------------------------------------
# Proxy catch-all (must be registered last)
# ---------------------------------------------------------------------------

@app.api_route(
    "/{full_path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
    include_in_schema=False,
)
async def proxy(request: Request, full_path: str):
    parts = full_path.split("/", 1)
    subpath = parts[0]
    rest = parts[1] if len(parts) > 1 else ""

    cfg = manager.get(subpath)
    if cfg is None:
        raise HTTPException(404, f"No proxy configured for /{subpath}")

    try:
        await asyncio.get_running_loop().run_in_executor(None, manager.ensure_running, cfg)
    except RuntimeError as e:
        raise HTTPException(502, str(e))

    cfg.last_activity = time.time()
    cfg.active_requests += 1
    req_start = time.time()
    log_path = f"/{rest}" + (f"?{request.url.query}" if request.url.query else "")

    url = f"{cfg.backend_url}/{rest}"
    if request.url.query:
        url += f"?{request.url.query}"

    fwd_headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in HOP_BY_HOP and k.lower() != "host"
    }

    try:
        body = await request.body()
    except ClientDisconnect:
        cfg.active_requests -= 1
        return Response(status_code=499)

    req_ct = request.headers.get("content-type", "")
    req_body_str: Optional[str] = None
    if body and _is_text_ct(req_ct):
        req_body_str = body[:MAX_BODY_CAPTURE].decode("utf-8", errors="replace")
        if len(body) > MAX_BODY_CAPTURE:
            req_body_str += f"\n\n[…{len(body) - MAX_BODY_CAPTURE} bytes truncated]"

    req_id = cfg.log_request_start(request.method, log_path, req_ct, req_body_str)
    asyncio.create_task(_broadcast())

    client = httpx.AsyncClient(timeout=None)
    try:
        upstream_req = client.build_request(
            method=request.method,
            url=url,
            headers=fwd_headers,
            content=body,
        )
        upstream_resp = await client.send(upstream_req, stream=True)
    except Exception as exc:
        await client.aclose()
        cfg.active_requests -= 1
        cfg.container_up = None  # force re-check: container may have crashed
        cfg.log_request_end(req_id, 502, round((time.time() - req_start) * 1000))
        asyncio.create_task(_broadcast())
        raise HTTPException(502, str(exc))

    resp_headers = {
        k: v for k, v in upstream_resp.headers.items()
        if k.lower() not in HOP_BY_HOP
    }

    status_code = upstream_resp.status_code
    resp_capture: dict = {"chunks": [], "total": 0}

    async def _stream():
        try:
            async for chunk in upstream_resp.aiter_bytes():
                yield chunk
                if resp_capture["total"] < MAX_BODY_CAPTURE:
                    take = min(len(chunk), MAX_BODY_CAPTURE - resp_capture["total"])
                    resp_capture["chunks"].append(chunk[:take])
                resp_capture["total"] += len(chunk)
                cfg.last_activity = time.time()
        except httpx.ReadError:
            pass  # upstream closed connection after sending all data
        finally:
            cfg.active_requests -= 1
            resp_ct = upstream_resp.headers.get("content-type", "")
            resp_body_str: Optional[str] = None
            if _is_text_ct(resp_ct) and resp_capture["chunks"]:
                captured = b"".join(resp_capture["chunks"]).decode("utf-8", errors="replace")
                if resp_capture["total"] > MAX_BODY_CAPTURE:
                    captured += f"\n\n[…{resp_capture['total'] - MAX_BODY_CAPTURE} bytes truncated]"
                resp_body_str = captured
            cfg.log_request_end(
                req_id, status_code,
                round((time.time() - req_start) * 1000),
                resp_content_type=resp_ct,
                resp_body=resp_body_str,
                resp_size=resp_capture["total"],
            )
            asyncio.create_task(_broadcast())
            await upstream_resp.aclose()
            await client.aclose()

    return StreamingResponse(
        _stream(),
        status_code=upstream_resp.status_code,
        headers=resp_headers,
        media_type=upstream_resp.headers.get("content-type"),
    )
