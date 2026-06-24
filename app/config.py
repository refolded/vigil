import os
from pathlib import Path
from typing import FrozenSet

DATA_FILE = Path(os.environ.get("DATA_FILE", "/data/proxies.json"))
STARTUP_TIMEOUT = int(os.environ.get("STARTUP_TIMEOUT", "120"))

MAX_BODY_CAPTURE = 8 * 1024
MAX_MEDIA_CAPTURE = 4 * 1024 * 1024

HOP_BY_HOP: FrozenSet[str] = frozenset({
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade",
})

RESERVED: FrozenSet[str] = frozenset({"api"})
