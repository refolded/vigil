import asyncio
import io
import json
import logging
import re
import time
import wave
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional, Set

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.requests import ClientDisconnect

from .config import HOP_BY_HOP, MAX_BODY_CAPTURE, MAX_MEDIA_CAPTURE, RESERVED
from .manager import ProxyManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent.parent / "static"


# ---------------------------------------------------------------------------
# TTS helpers
# ---------------------------------------------------------------------------

def split_tts_text(text: str, mode: str) -> list[str]:
    """Split TTS input into chunks for parallel generation."""
    text = text.strip()
    if not text:
        return [text]
    if mode == "paragraph":
        chunks = [p.strip() for p in re.split(r'\n\n+', text)]
    else:  # sentence
        chunks = re.split(r'(?<=[.!?])["\']?\s+', text)
    chunks = [c for c in (c.strip() for c in chunks) if c]
    if not chunks:
        return [text]
    result: list[str] = []
    for chunk in chunks:
        if result and len(result[-1]) < 25:
            result[-1] += ' ' + chunk
        else:
            result.append(chunk)
    return result


def _strip_id3(data: bytes) -> bytes:
    """Strip ID3v2 tag from start of MP3 data, if present."""
    if data[:3] == b'ID3':
        size = (
            (data[6] & 0x7F) << 21 |
            (data[7] & 0x7F) << 14 |
            (data[8] & 0x7F) << 7  |
            (data[9] & 0x7F)
        ) + 10
        return data[size:]
    return data


def concatenate_audio(parts: list[bytes]) -> bytes:
    """Concatenate audio blobs. WAV-aware merge when possible, MP3-safe otherwise."""
    if len(parts) == 1:
        return parts[0]
    params = None
    all_frames: list[bytes] = []
    for part in parts:
        try:
            with wave.open(io.BytesIO(part), 'rb') as w:
                if params is None:
                    params = w.getparams()
                all_frames.append(w.readframes(w.getnframes()))
        except Exception:
            all_frames = []
            break
    if params and len(all_frames) == len(parts):
        out = io.BytesIO()
        with wave.open(out, 'wb') as w:
            w.setparams(params)
            for f in all_frames:
                w.writeframes(f)
        return out.getvalue()
    return parts[0] + b"".join(_strip_id3(p) for p in parts[1:])


# ---------------------------------------------------------------------------
# SSE broadcast
# ---------------------------------------------------------------------------

_sse_queues: Set[asyncio.Queue] = set()
_main_loop: Optional[asyncio.AbstractEventLoop] = None

manager = ProxyManager()


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _main_loop
    _main_loop = asyncio.get_running_loop()
    manager._on_change = _schedule_broadcast
    yield


def _is_text_ct(ct: str) -> bool:
    ct = ct.lower()
    return any(t in ct for t in ("json", "text/", "xml", "html", "yaml"))


def _is_media_ct(ct: str) -> bool:
    ct = ct.lower()
    return ct.startswith(("audio/", "image/", "video/"))


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="Vigil", docs_url="/api/docs", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def ui():
    return (STATIC_DIR / "index.html").read_text()


# ---------------------------------------------------------------------------
# API — health
# ---------------------------------------------------------------------------

@app.get("/api/health", tags=["health"])
async def health():
    return {"ok": True}


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
    max_concurrency: int = 0
    health_path: str = "/"
    tts_split_mode: str = "none"
    enabled: bool = True


class ProxyPatch(BaseModel):
    idle_timeout: Optional[int] = None
    auto_unload: Optional[bool] = None
    max_concurrency: Optional[int] = None
    health_path: Optional[str] = None
    tts_split_mode: Optional[str] = None
    enabled: Optional[bool] = None


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
    cfg = manager.add(
        subpath, body.container, body.backend_url,
        body.idle_timeout, body.auto_unload,
        body.max_concurrency, body.health_path,
        body.tts_split_mode, body.enabled,
    )
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


@app.post("/api/proxies/{subpath:path}/restart", tags=["proxies"])
async def restart_proxy(subpath: str):
    cfg = manager.get(subpath)
    if cfg is None:
        raise HTTPException(404, f"No proxy for /{subpath}")
    try:
        await asyncio.get_running_loop().run_in_executor(None, manager.restart, cfg)
    except RuntimeError as e:
        raise HTTPException(502, str(e))
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


@app.get("/api/proxies/{subpath}/requests/{req_id}/preview", include_in_schema=False)
async def preview_response(subpath: str, req_id: int):
    cfg = manager.get(subpath)
    if cfg is None:
        raise HTTPException(404)
    data = cfg._preview_cache.get(req_id)
    if data is None:
        raise HTTPException(404, "Preview not cached")
    ct = "application/octet-stream"
    for entry in cfg.request_log:
        if entry.get("id") == req_id:
            ct = entry.get("resp_ct", ct)
            break
    return Response(content=data, media_type=ct)


# ---------------------------------------------------------------------------
# SSE
# ---------------------------------------------------------------------------

@app.get("/api/events", include_in_schema=False)
async def sse_events(request: Request):
    q: asyncio.Queue = asyncio.Queue(maxsize=20)
    _sse_queues.add(q)

    async def stream():
        try:
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
    if not cfg.enabled:
        raise HTTPException(503, f"Proxy /{subpath} is disabled")

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

    client_ip = request.client.host if request.client else ""
    req_headers = dict(request.headers)

    req_id = cfg.log_request_start(
        request.method, log_path,
        client_ip=client_ip,
        req_content_type=req_ct,
        req_body=req_body_str,
        req_headers=req_headers,
    )
    asyncio.create_task(_broadcast())

    sem_acquired = False
    if cfg.max_concurrency > 0:
        if cfg._semaphore is None:
            cfg._semaphore = asyncio.Semaphore(cfg.max_concurrency)
        cfg.queued_requests += 1
        asyncio.create_task(_broadcast())
        try:
            await asyncio.wait_for(cfg._semaphore.acquire(), timeout=300.0)
            sem_acquired = True
        except asyncio.TimeoutError:
            cfg.queued_requests -= 1
            cfg.active_requests -= 1
            cfg.log_request_end(req_id, 503, round((time.time() - req_start) * 1000))
            asyncio.create_task(_broadcast())
            raise HTTPException(503, "Request queue timeout after 300s")
        cfg.queued_requests -= 1
        asyncio.create_task(_broadcast())

    # ------------------------------------------------------------------ #
    # TTS SPLIT PATH                                                       #
    # ------------------------------------------------------------------ #
    if cfg.tts_split_mode and cfg.tts_split_mode != "none":
        body_json: Optional[dict] = None
        input_text = ""
        try:
            body_json = json.loads(body)
            input_text = body_json.get("input") or body_json.get("text", "")
        except Exception:
            pass

        if input_text and body_json is not None:
            chunks = split_tts_text(input_text, cfg.tts_split_mode)
            if len(chunks) > 1:
                input_key = "input" if "input" in body_json else "text"
                chunk_headers = {
                    k: v for k, v in fwd_headers.items()
                    if k.lower() != "content-length"
                }
                chunk_headers["content-type"] = "application/json"

                async def fetch_chunk(chunk_text: str) -> tuple[bytes, str]:
                    chunk_body = {**body_json, input_key: chunk_text}
                    async with httpx.AsyncClient(timeout=300) as hc:
                        r = await hc.request(
                            method=request.method,
                            url=url,
                            headers=chunk_headers,
                            content=json.dumps(chunk_body).encode(),
                        )
                        r.raise_for_status()
                        return r.content, r.headers.get("content-type", "audio/wav")

                try:
                    chunk_results = await asyncio.gather(*[fetch_chunk(c) for c in chunks])
                except Exception as exc:
                    cfg.active_requests -= 1
                    if sem_acquired:
                        cfg._semaphore.release()
                    cfg.log_request_end(req_id, 502, round((time.time() - req_start) * 1000))
                    asyncio.create_task(_broadcast())
                    raise HTTPException(502, f"TTS chunk failed: {exc}")

                chunk_bytes = [b for b, _ in chunk_results]
                combined_ct = chunk_results[0][1]
                combined = concatenate_audio(chunk_bytes)
                cfg.active_requests -= 1
                if sem_acquired:
                    cfg._semaphore.release()
                cfg.last_activity = time.time()
                cfg.log_request_end(
                    req_id, 200,
                    round((time.time() - req_start) * 1000),
                    resp_content_type=combined_ct,
                    resp_size=len(combined),
                    resp_is_media=True,
                    resp_preview=combined[:MAX_MEDIA_CAPTURE],
                )
                asyncio.create_task(_broadcast())
                return Response(content=combined, media_type=combined_ct, status_code=200)

    # ------------------------------------------------------------------ #
    # NORMAL PROXY PATH (streaming)                                        #
    # ------------------------------------------------------------------ #
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
        if sem_acquired:
            cfg._semaphore.release()
        cfg.container_up = None
        cfg.log_request_end(req_id, 502, round((time.time() - req_start) * 1000))
        asyncio.create_task(_broadcast())
        raise HTTPException(502, str(exc))

    fwd_resp_headers = {
        k: v for k, v in upstream_resp.headers.items()
        if k.lower() not in HOP_BY_HOP
    }
    all_resp_headers = dict(upstream_resp.headers)

    status_code = upstream_resp.status_code
    resp_ct = upstream_resp.headers.get("content-type", "")
    is_media = _is_media_ct(resp_ct)

    capture_limit = MAX_MEDIA_CAPTURE if is_media else MAX_BODY_CAPTURE
    resp_capture: dict = {"chunks": [], "total": 0}

    async def _stream():
        try:
            async for chunk in upstream_resp.aiter_bytes():
                yield chunk
                if resp_capture["total"] < capture_limit:
                    take = min(len(chunk), capture_limit - resp_capture["total"])
                    resp_capture["chunks"].append(chunk[:take])
                resp_capture["total"] += len(chunk)
                cfg.last_activity = time.time()
        except httpx.ReadError:
            pass
        finally:
            cfg.active_requests -= 1
            if sem_acquired:
                cfg._semaphore.release()
            resp_body_str: Optional[str] = None
            resp_preview: Optional[bytes] = None
            if is_media and resp_capture["chunks"]:
                resp_preview = b"".join(resp_capture["chunks"])
            elif _is_text_ct(resp_ct) and resp_capture["chunks"]:
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
                resp_is_media=is_media,
                resp_headers=all_resp_headers,
                resp_preview=resp_preview,
            )
            asyncio.create_task(_broadcast())
            await upstream_resp.aclose()
            await client.aclose()

    return StreamingResponse(
        _stream(),
        status_code=upstream_resp.status_code,
        headers=fwd_resp_headers,
        media_type=resp_ct,
    )
