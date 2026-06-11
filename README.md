# Vigil

**watch. sleep. wake.**

Vigil is a transparent HTTP proxy that stops GPU (or any) Docker containers after an idle timeout, then cold-starts them on the next request. A web UI lets you manage multiple proxied containers, set per-container idle timeouts, and monitor live request traffic — all without touching the containers themselves.

## How it works

- Sits in front of your AI container on a port you choose
- Forwards all requests transparently — including streaming / SSE / chunked responses
- Tracks idle time per proxy; stops (or restarts) the container after N idle seconds
- On the next request, starts the container, waits for it to be ready, then forwards
- Proxy configs persist across Vigil restarts via a mounted volume

## Quick start

```bash
docker run -d \
  --network host \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v vigil-data:/data \
  ghcr.io/raslan/vigil:latest
```

Open **http://localhost:8474** for the web UI.

### docker-compose

```bash
docker compose up -d
```

## Web UI

| Section | What it does |
|---|---|
| Containers | All Docker containers on the host. Click **Proxy →** to pre-fill the form. |
| Active Proxies | Configured proxies with live idle bar, status, and last 10 requests. Click a row to expand. Click the subpath to copy the proxy URL. |
| Add Proxy | Pick container, set subpath, backend URL, idle timeout, and unload mode. |

Requests to `http://localhost:8474/mysubpath/...` are forwarded to the configured backend, stripping the subpath prefix.

## Idle modes

| Toggle | Behavior |
|---|---|
| **Off** (default) | On idle: restart container — model reloads, VRAM stays allocated |
| **On** (Unload on idle) | On idle: stop container — VRAM freed, cold-starts on next request |

The manual **⏹ Unload / ↺ Restart** button respects the same toggle.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `STARTUP_TIMEOUT` | `120` | Max seconds to wait for a container to become ready after start |
| `DATA_FILE` | `/data/proxies.json` | Where proxy configs are persisted |

## Notes

- Requires `network_mode: host` (Linux) so `localhost:<port>` inside the Vigil container resolves to the host's network stack.
- Docker socket is mounted so Vigil can start/stop containers.
- `/` and `/api/...` are reserved paths and cannot be used as proxy subpaths.
- Alpine.js and Tailwind CSS are bundled into the image at build time — the UI works fully offline.
