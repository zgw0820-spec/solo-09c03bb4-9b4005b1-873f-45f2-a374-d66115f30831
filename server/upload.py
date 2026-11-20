"""TFTP write (WRQ) sessions and the on-disk log archive.

A diskless rescue machine uploads its boot log as ``<work-order>.log`` in
octet mode, fixed 512-byte DATA blocks, plain RFC 1350 stop-and-wait (no
option negotiation on writes).

Guarantees implemented here:

* one active upload per work order; an already archived log is never
  overwritten (the second WRQ is refused on port 69 with a TFTP ERROR)
* bytes land in a private staging file and the finished log is *atomically*
  renamed into the archive only after the final block is fsynced, so a
  reader can never observe a half-written log and the archive is written
  exactly once
* the final block is ACKed only after that rename succeeded; a write
  failure answers the client with a TFTP ERROR and removes the staging file
* a lost final ACK is retransmitted, within the retry budget, from the
  original transfer port; duplicate final DATA gets the same ACK again
* an idle/aborted upload deletes its staging file and releases the work
  order so the ticket can be reused immediately
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import struct
import time

from common.protocol import (
    BLKSIZE,
    ERR_ACCESS_VIOLATION,
    ERR_DISK_FULL,
    ERR_FILE_EXISTS,
    ERR_ILLEGAL_OP,
    OP_DATA,
    OP_ERROR,
    OP_WRQ,
    build_ack,
    build_error,
    wire_num,
)

log = logging.getLogger("tftp.upload")

# Work orders are safe path components only; the requested file name must be
# exactly "<work-order>.log".
WORK_ORDER_RE = re.compile(r"[A-Za-z0-9_-]{1,64}")
LOG_SUFFIX = ".log"


class WorkOrderError(ValueError):
    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def parse_log_filename(filename: str) -> str:
    """Return the validated work order for a ``<work-order>.log`` request."""
    if not filename or not filename.endswith(LOG_SUFFIX) or filename == LOG_SUFFIX:
        raise WorkOrderError(ERR_ILLEGAL_OP, "filename must be <work-order>.log")
    work_order = filename[: -len(LOG_SUFFIX)]
    if not WORK_ORDER_RE.fullmatch(work_order) or work_order in (".", ".."):
        raise WorkOrderError(ERR_ILLEGAL_OP, "invalid work order id")
    return work_order


class ArchiveStore:
    """Owns the staging directory, the archive directory and reservations."""

    def __init__(self, archive_dir: str, staging_dir: str):
        self.archive_dir = archive_dir
        self.staging_dir = staging_dir
        os.makedirs(self.archive_dir, exist_ok=True)
        os.makedirs(self.staging_dir, exist_ok=True)
        self.active: dict[str, "UploadSession"] = {}
        self.history: list[dict] = []

    def startup_cleanup(self) -> int:
        """Remove staging files left behind by a previous hard shutdown."""
        removed = 0
        for name in os.listdir(self.staging_dir):
            try:
                os.remove(os.path.join(self.staging_dir, name))
                removed += 1
            except OSError:
                log.exception("failed to remove stale staging file %s", name)
        if removed:
            log.warning("removed %d stale staging file(s) on startup", removed)
        return removed

    def archive_path(self, filename: str) -> str:
        return os.path.join(self.archive_dir, filename)

    def is_archived(self, filename: str) -> bool:
        return os.path.isfile(self.archive_path(filename))

    def reserve(self, session: "UploadSession") -> None:
        """Reserve the work order or raise :class:`WorkOrderError`."""
        filename = session.filename
        if self.is_archived(filename):
            raise WorkOrderError(ERR_FILE_EXISTS, f"{filename} already archived")
        if session.work_order in self.active:
            raise WorkOrderError(
                ERR_ACCESS_VIOLATION,
                f"upload for work order {session.work_order} already in progress",
            )
        self.active[session.work_order] = session

    def release(self, session: "UploadSession") -> None:
        if self.active.get(session.work_order) is session:
            del self.active[session.work_order]

    def staging_path(self, work_order: str) -> str:
        return os.path.join(self.staging_dir, f"{work_order}.part")

    def list_archives(self) -> list[dict]:
        out = []
        for name in sorted(os.listdir(self.archive_dir)):
            path = self.archive_path(name)
            if not os.path.isfile(path):
                continue
            st = os.stat(path)
            out.append({
                "filename": name,
                "work_order": name[:-len(LOG_SUFFIX)] if name.endswith(LOG_SUFFIX) else name,
                "size_bytes": st.st_size,
                "modified_at": int(st.st_mtime),
            })
        return out

    def record_history(self, stats: dict):
        self.history.append(stats)


class _UploadProtocol(asyncio.DatagramProtocol):
    def __init__(self, session: "UploadSession"):
        self.session = session

    def datagram_received(self, data, addr):
        self.session.on_datagram(data, addr)

    def error_received(self, exc):  # ICMP on unconnected sockets: ignore
        pass


class UploadSession:
    """One stop-and-wait WRQ transfer on its own ephemeral transfer port."""

    def __init__(self, store: ArchiveStore, service, client, work_order: str,
                 timeout: float, max_retries: int):
        self.store = store
        self.service = service
        self.client = client
        self.work_order = work_order
        self.filename = work_order + LOG_SUFFIX
        self.timeout = timeout
        self.max_retries = max_retries
        self.expected = 1          # next absolute (1-based) block index wanted
        self.last_acked = 0        # highest contiguous absolute block ACKed
        self.bytes_written = 0
        self.digest = hashlib.sha256()
        self.retries = 0
        self.transport = None
        self.port = None
        self.fh = None
        self.staging_path = store.staging_path(work_order)
        self.finalized = False    # the archive write happens at most once
        self.done = False
        self.lingering = False
        self.final_wire = None
        self._timer = None
        self.started = time.monotonic()
        self.stats = {
            "work_order": work_order,
            "filename": self.filename,
            "client": f"{client[0]}:{client[1]}",
            "port": None,
            "data_blocks": 0,
            "duplicate_data": 0,
            "ignored_packets": 0,
            "acks_sent": 0,
            "ack_retransmits": 0,
            "final_ack_sent": 0,
            "timeouts": 0,
            "bytes_written": 0,
            "sha256": None,
            "result": None,
        }

    async def start(self):
        loop = asyncio.get_running_loop()
        # truncate: a stale .part for this ticket should have been removed,
        # but never append onto unknown bytes
        self.fh = open(self.staging_path, "wb")
        try:
            transport, _ = await loop.create_datagram_endpoint(
                lambda: _UploadProtocol(self),
                local_addr=(self.service.bind_host, 0),
            )
        except OSError:
            self._finish("aborted: transfer socket setup failure")
            raise
        if self.done:  # server stopped while the socket was being bound
            transport.close()
            return
        self.transport = transport
        self.port = transport.get_extra_info("sockname")[1]
        self.stats["port"] = self.port
        log.info("upload client=%s session start tid=%d file=%s",
                 self.client, self.port, self.filename)
        self._send_ack(0, reason="initial ACK 0")
        self._rearm()

    # --------------------------------------------------------------- sending
    def _sendto(self, payload: bytes):
        if self.transport is not None:
            self.transport.sendto(payload, self.client)

    def _send_ack(self, wire: int, reason: str = ""):
        self.stats["acks_sent"] += 1
        if reason:
            log.info("upload client=%s tid=%s ACK %d (%s)",
                     self.client, self.port, wire, reason)
        self._sendto(build_ack(wire))

    def _send_error_and_abort(self, code: int, message: str):
        log.warning("upload client=%s tid=%s ERROR %d: %s",
                    self.client, self.port, code, message)
        if self.transport is not None:
            self._sendto(build_error(code, message))
        self._finish(f"aborted: {message}")

    # ------------------------------------------------------------- receiving
    def on_datagram(self, data: bytes, addr):
        if self.done or addr != self.client or len(data) < 2:
            return
        op = struct.unpack("!H", data[:2])[0]
        if op == OP_ERROR:
            self._finish("aborted: client sent ERROR")
            return
        if op == OP_WRQ:
            # duplicate WRQ on the transfer port: re-issue ACK 0
            self.retries = 0
            self._send_ack(0, reason="duplicate WRQ")
            self._rearm()
            return
        if op != OP_DATA or len(data) < 4:
            self.stats["ignored_packets"] += 1
            return
        wire = struct.unpack("!H", data[2:4])[0]
        payload = data[4:]
        if len(payload) > BLKSIZE:
            self._send_error_and_abort(ERR_ILLEGAL_OP,
                                       f"DATA block exceeds {BLKSIZE} bytes")
            return
        # unwrap the 16-bit wire number into the absolute index space; anchor
        # on the highest contiguous block so a duplicate of the final block
        # (after ``expected`` has advanced) is recognised correctly
        candidate = self.last_acked + (wire - wire_num(self.last_acked)) % 65536
        if candidate < self.expected:
            # duplicate of an already handled block: repeat its ACK
            self.stats["duplicate_data"] += 1
            dup_wire = wire_num(candidate)
            if self.finalized and dup_wire == self.final_wire:
                self.stats["final_ack_sent"] += 1
            self._send_ack(dup_wire, reason=f"duplicate block {candidate}")
            self.retries = 0
            self._rearm()
            return
        if candidate > self.expected:
            # gap: stop-and-wait clients should not do this; re-affirm the
            # highest contiguous block we hold
            self.stats["ignored_packets"] += 1
            self._send_ack(wire_num(self.last_acked), reason="gap re-ACK")
            return

        self._handle_in_order(payload)

    def _handle_in_order(self, payload: bytes):
        index = self.expected
        is_final = len(payload) < BLKSIZE
        try:
            assert self.fh is not None
            self.fh.write(payload)
            self.bytes_written += len(payload)
            self.digest.update(payload)
            if is_final:
                self.fh.flush()
                os.fsync(self.fh.fileno())
                self.fh.close()
                self.fh = None
                self._commit_archive()   # may raise on disk failure
        except OSError as exc:
            self._send_error_and_abort(ERR_DISK_FULL, f"write failed: {exc}")
            return

        self.last_acked = index
        self.expected = index + 1
        self.stats["data_blocks"] += 1
        self.stats["bytes_written"] = self.bytes_written

        if is_final:
            # the complete log is on disk and visible in the archive BEFORE
            # the final block is acknowledged
            self.final_wire = wire_num(index)
            self.lingering = True
            self.stats["final_ack_sent"] += 1
            self._send_ack(self.final_wire, reason="final block")
            log.info("upload client=%s tid=%s archived %s (%d bytes)",
                     self.client, self.port, self.filename, self.bytes_written)
            self.retries = 0
            # keep retransmitting the final ACK within the retry window, then
            # release the port; the work order was already freed at commit
            self._rearm_final_grace()
        else:
            self._send_ack(wire_num(index))
            self.retries = 0
            self._rearm()

    def _rearm_final_grace(self):
        """Schedule one more final-ACK retransmit, or close after the budget."""
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if self.done:
            return
        if self.retries >= self.max_retries:
            self._finish("completed (final ACK retransmit window closed)")
            return
        self._timer = asyncio.get_running_loop().call_later(
            self.timeout, self._on_timeout
        )

    def _commit_archive(self):
        """Atomically publish the staging file into the archive, once only."""
        if self.finalized:
            return
        target = self.store.archive_path(self.filename)
        # last defensive check: never overwrite an existing archive
        if os.path.exists(target):
            raise OSError(f"{self.filename} already exists")
        os.replace(self.staging_path, target)
        self.finalized = True
        self.stats["sha256"] = self.digest.hexdigest()
        self.store.release(self)

    # ------------------------------------------------------ timeout / finish
    def _rearm(self):
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if not self.done:
            self._timer = asyncio.get_running_loop().call_later(
                self.timeout, self._on_timeout
            )

    def _on_timeout(self):
        if self.done:
            return
        self.retries += 1
        self.stats["timeouts"] += 1
        if self.retries > self.max_retries:
            if self.finalized:
                # archive is safe; just give up re-ACKing and release the port
                self._finish("completed (final ACK retransmit window closed)")
            else:
                log.warning(
                    "upload client=%s tid=%s timed out after %d retries: "
                    "removing partial %s and releasing work order %s",
                    self.client, self.port, self.max_retries,
                    self.staging_path, self.work_order,
                )
                self._finish("aborted: idle timeout, partial upload removed")
            return
        if self.finalized:
            # lost final ACK: retransmit it from the SAME transfer port
            self.stats["ack_retransmits"] += 1
            self.stats["final_ack_sent"] += 1
            self._send_ack(self.final_wire, reason=f"final ACK retry {self.retries}")
            self._rearm_final_grace()
        else:
            # the client probably missed an ACK: repeat the last one
            self.stats["ack_retransmits"] += 1
            self._send_ack(wire_num(self.last_acked),
                           reason=f"ACK retry {self.retries}")
            self._rearm()

    def _finish(self, result: str):
        if self.done:
            return
        self.done = True
        self.stats["result"] = result
        self.stats["elapsed_s"] = round(time.monotonic() - self.started, 3)
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if self.fh is not None:
            try:
                self.fh.close()
            except OSError:
                pass
            self.fh = None
        if not self.finalized and os.path.exists(self.staging_path):
            try:
                os.remove(self.staging_path)  # delete the half product
            except OSError:
                log.exception("could not remove staging file %s", self.staging_path)
        if self.transport is not None:
            self.transport.close()  # release the transfer port
        self.store.release(self)
        log.info("upload client=%s tid=%s %s (port released)",
                 self.client, self.port, result)
        self.service.upload_finished(self)


def reject(transport, addr, code: int, message: str) -> None:
    """Send a TFTP ERROR from the port-69 front door and log it."""
    log.info("upload refused client=%s error %d: %s", addr, code, message)
    if transport is not None:
        transport.sendto(build_error(code, message), addr)
