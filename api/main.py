"""FastAPI front-end: real TFTP downloads (rescue image) and WRQ log uploads.

Open /docs (Swagger UI):

* ``POST /download`` runs an actual UDP TFTP download of the fixed rescue
  image and verifies its SHA-256
* ``POST /upload`` runs an actual UDP TFTP **WRQ upload** of a boot log
  named ``<work-order>.log`` against the TFTP server container, with drill
  knobs for packet loss and a lost final ACK
* ``GET /logs`` lists archived logs; ``GET /logs/{work_order}/info`` shows
  one; ``GET /logs/{work_order}/download`` fetches the archived bytes
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import re
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import Response

from api.tftp_client import ClientConfig, TftpClient
from common.imagegen import DEFAULT_SEED, DEFAULT_SIZE, image_sha256
from common.protocol import BLKSIZE, MAX_WINDOW, total_blocks_for

TFTP_SERVER_HOST = os.environ.get("TFTP_SERVER_HOST", "127.0.0.1")
TFTP_SERVER_PORT = int(os.environ.get("TFTP_SERVER_PORT", "6969"))
IMAGE_SIZE = int(os.environ.get("IMAGE_SIZE", str(DEFAULT_SIZE)))
IMAGE_SEED = os.environ.get("IMAGE_SEED", DEFAULT_SEED)
IMAGE_FILENAME = os.environ.get("IMAGE_FILENAME", "rescue.img")
# shared volume with the tftp-server container; the API reads finished logs
LOG_ARCHIVE_DIR = os.environ.get("UPLOAD_ARCHIVE_DIR", "/data/archive")
LOG_SUFFIX = ".log"
WORK_ORDER_RE = re.compile(r"[A-Za-z0-9_-]{1,64}")

_hash_cache: str | None = None


def expected_sha256() -> str:
    global _hash_cache
    if _hash_cache is None:
        _hash_cache = image_sha256(IMAGE_SIZE, IMAGE_SEED)
    return _hash_cache


def make_log_payload(work_order: str, size: int, seed: int) -> bytes:
    """Deterministic, text-like boot-log payload for an upload drill."""
    size = max(0, min(size, 8 * 1024 * 1024))
    out = bytearray()
    counter = 0
    while len(out) < size:
        line = (f"[{counter:08d}] rescue boot log ticket={work_order} "
                f"seed={seed} {hashlib.sha256(f'{work_order}:{seed}:{counter}'.encode()).hexdigest()}\n")
        out.extend(line.encode())
        counter += 1
    return bytes(out[:size])


def validate_work_order(work_order: str) -> str:
    if not WORK_ORDER_RE.fullmatch(work_order):
        raise HTTPException(status_code=400,
                            detail="work order may contain only A-Z a-z 0-9 _ - (1-64 chars)")
    return work_order


def archive_path(work_order: str) -> str:
    return os.path.join(LOG_ARCHIVE_DIR, work_order + LOG_SUFFIX)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # hash the image once, off the event loop, before serving requests
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, expected_sha256)
    os.makedirs(LOG_ARCHIVE_DIR, exist_ok=True)
    yield


app = FastAPI(
    title="Rescue TFTP: image download + boot-log upload",
    description=(
        "Real UDP TFTP against "
        f"`{TFTP_SERVER_HOST}:{TFTP_SERVER_PORT}`. Downloads serve the fixed "
        "rescue image (octet, 512-byte blocks, RFC 7440 windowsize capped at "
        "16). Uploads accept WRQ of `<work-order>.log`; finished logs are "
        "archived by the TFTP server and can be fetched here by work order."
    ),
    version="2.0.0",
    lifespan=lifespan,
)


@app.get("/")
def root():
    return {"service": "rescue tftp download + boot-log upload", "docs": "/docs"}


@app.get("/health")
def health():
    return {"status": "ok"}


# ============================================================ image download
@app.get("/image/info")
def image_info():
    return {
        "filename": IMAGE_FILENAME,
        "size_bytes": IMAGE_SIZE,
        "sha256": expected_sha256(),
        "blksize": BLKSIZE,
        "total_blocks": total_blocks_for(IMAGE_SIZE),
        "ends_with_empty_block": IMAGE_SIZE % BLKSIZE == 0,
        "tftp_server": f"{TFTP_SERVER_HOST}:{TFTP_SERVER_PORT}",
    }


@app.post("/download")
async def download(
    window: int = Query(MAX_WINDOW, ge=1, le=64,
                        description="windowsize to request; the server caps it at 16"),
    loss: float = Query(0.0, ge=0.0, le=0.95,
                        description="probability of dropping each inbound DATA packet"),
    ack_loss: float = Query(0.0, ge=0.0, le=1.0,
                            description="probability of dropping each outbound ACK"),
    drop_first_oack: bool = Query(False,
                                  description="drop the first OACK to force an RRQ retransmission"),
    seed: int = Query(1, description="RNG seed for the loss simulation"),
    timeout: float = Query(1.0, ge=0.1, le=10.0, description="per-packet wait, seconds"),
    max_retries: int = Query(8, ge=1, le=30, description="give up after this many retries"),
):
    cfg = ClientConfig(
        host=TFTP_SERVER_HOST,
        port=TFTP_SERVER_PORT,
        filename=IMAGE_FILENAME,
        window=window,
        timeout=timeout,
        max_retries=max_retries,
        loss=loss,
        ack_loss=ack_loss,
        drop_first_oack=drop_first_oack,
        seed=seed,
    )
    result = await TftpClient(cfg).download()
    received = hashlib.sha256(result.data).hexdigest() if result.ok else None
    expected = expected_sha256()
    return {
        "ok": result.ok,
        "error": result.error,
        "match": result.ok and received == expected,
        "sha256": received,
        "expected_sha256": expected,
        "bytes_received": len(result.data),
        **result.stats,
    }


# ============================================================== log uploads
@app.post("/upload")
async def upload(
    work_order: str = Query(..., min_length=1, max_length=64,
                            description="ticket id; the TFTP file is <work-order>.log"),
    payload_size: int = Query(1200, ge=0, le=8 * 1024 * 1024,
                              description="boot-log payload size in bytes (0 tests the empty block)"),
    seed: int = Query(1, description="RNG / content seed"),
    data_loss: float = Query(0.0, ge=0.0, le=0.95,
                             description="probability of dropping each outbound DATA packet"),
    drop_final_ack: bool = Query(
        False,
        description="swallow the first ACK of the final block to force a "
                    "same-port final-ACK retransmission",
    ),
    timeout: float = Query(1.0, ge=0.1, le=10.0, description="per-packet wait, seconds"),
    max_retries: int = Query(8, ge=1, le=30, description="give up after this many retries"),
):
    """Trigger a real UDP TFTP WRQ upload of ``<work-order>.log``."""
    validate_work_order(work_order)
    filename = work_order + LOG_SUFFIX
    payload = make_log_payload(work_order, payload_size, seed)
    cfg = ClientConfig(
        host=TFTP_SERVER_HOST,
        port=TFTP_SERVER_PORT,
        filename=filename,
        timeout=timeout,
        max_retries=max_retries,
        data_loss=data_loss,
        drop_final_ack=drop_final_ack,
        seed=seed,
    )
    result = await TftpClient(cfg).upload(payload)
    archived = os.path.isfile(archive_path(work_order))
    local_sha = hashlib.sha256(payload).hexdigest()
    remote_sha = None
    remote_size = None
    if archived:
        with open(archive_path(work_order), "rb") as fh:
            blob = fh.read()
        remote_sha = hashlib.sha256(blob).hexdigest()
        remote_size = len(blob)
    return {
        "ok": result.ok,
        "error": result.error,
        "work_order": work_order,
        "filename": filename,
        "payload_size": len(payload),
        "payload_sha256": local_sha,
        "archived": archived,
        "archive_match": archived and remote_sha == local_sha,
        "archived_size_bytes": remote_size,
        "archived_sha256": remote_sha,
        **result.stats,
    }


@app.get("/logs")
def list_logs():
    """List every archived boot log (partial uploads are never visible)."""
    if not os.path.isdir(LOG_ARCHIVE_DIR):
        return {"archive_dir": LOG_ARCHIVE_DIR, "logs": []}
    logs = []
    for name in sorted(os.listdir(LOG_ARCHIVE_DIR)):
        if not name.endswith(LOG_SUFFIX):
            continue
        path = os.path.join(LOG_ARCHIVE_DIR, name)
        if not os.path.isfile(path):
            continue
        st = os.stat(path)
        with open(path, "rb") as fh:
            digest = hashlib.sha256(fh.read()).hexdigest()
        logs.append({
            "work_order": name[:-len(LOG_SUFFIX)],
            "filename": name,
            "size_bytes": st.st_size,
            "sha256": digest,
            "modified_at": int(st.st_mtime),
        })
    return {"archive_dir": LOG_ARCHIVE_DIR, "count": len(logs), "logs": logs}


@app.get("/logs/{work_order}/info")
def log_info(work_order: str):
    validate_work_order(work_order)
    path = archive_path(work_order)
    if not os.path.isfile(path):
        raise HTTPException(status_code=404,
                            detail=f"no archived log for work order {work_order!r}")
    with open(path, "rb") as fh:
        blob = fh.read()
    return {
        "work_order": work_order,
        "filename": work_order + LOG_SUFFIX,
        "size_bytes": len(blob),
        "sha256": hashlib.sha256(blob).hexdigest(),
        "modified_at": int(os.stat(path).st_mtime),
    }


@app.get("/logs/{work_order}/download")
def log_download(work_order: str):
    """Fetch the archived boot-log bytes by work order."""
    validate_work_order(work_order)
    path = archive_path(work_order)
    if not os.path.isfile(path):
        raise HTTPException(status_code=404,
                            detail=f"no archived log for work order {work_order!r}")
    with open(path, "rb") as fh:
        blob = fh.read()
    return Response(
        content=blob,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{work_order}.log"'},
    )
