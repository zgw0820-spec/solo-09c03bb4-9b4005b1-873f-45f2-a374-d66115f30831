"""End-to-end checks for TFTP WRQ boot-log uploads (no docker needed).

Run from the repo root:  python3 tests/e2e_upload.py

Covers: basic upload + exact-multiple empty terminator, same-work-order
concurrency, archived-log overwrite refusal, lost final ACK retransmitted
from the original transfer port, idle-timeout cleanup + ticket reuse,
disk/rename failure -> TFTP ERROR, DATA loss retransmission, archive
visibility, and the FastAPI upload/fetch surface.
"""
import asyncio
import hashlib
import os
import shutil
import struct
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.imagegen import generate_image
from common.protocol import (
    BLKSIZE, OP_ACK, OP_ERROR, OP_WRQ, build_error, build_wrq,
)
from server.tftp_server import TftpServerService
from api.tftp_client import ClientConfig, TftpClient

RESULTS = []


def check(name, cond, detail=""):
    RESULTS.append((name, bool(cond)))
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""))


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class _RawProto(asyncio.DatagramProtocol):
    def __init__(self):
        self.queue = asyncio.Queue()

    def datagram_received(self, data, addr):
        self.queue.put_nowait((data, addr))

    def error_received(self, exc):
        pass


class RawUploader:
    """Minimal scriptable WRQ client for scenarios the high-level client
    cannot express (concurrency, mid-transfer silence, final-ACK swallow)."""

    def __init__(self, server_addr, filename: str, timeout: float = 2.0):
        self.server = server_addr
        self.filename = filename
        self.timeout = timeout
        self.transport = None
        self.proto = None
        self.transfer_port = None
        self.packets = []

    async def __aenter__(self):
        loop = asyncio.get_running_loop()
        self.transport, self.proto = await loop.create_datagram_endpoint(
            _RawProto, local_addr=("127.0.0.1", 0))
        return self

    async def __aexit__(self, *exc):
        self.transport.close()

    async def recv(self):
        data, addr = await asyncio.wait_for(self.proto.queue.get(), self.timeout)
        self.packets.append((data, addr))
        if self.transfer_port is None:
            self.transfer_port = addr[1]
        return data, addr

    def send(self, pkt: bytes, addr=None):
        self.transport.sendto(pkt, addr or self.server)

    async def wait_ack0(self):
        while True:
            data, addr = await self.recv()
            op = struct.unpack("!H", data[:2])[0]
            if op == OP_ERROR:
                code, msg = data[2:4], data[4:].rstrip(b"\x00").decode("ascii", "replace")
                return ("ERROR", struct.unpack("!H", code)[0], msg, addr)
            if op == OP_ACK and len(data) >= 4 and struct.unpack("!H", data[2:4])[0] == 0:
                return ("ACK0", None, None, addr)

    @staticmethod
    def data_pkt(index: int, payload: bytes) -> bytes:
        return struct.pack("!HH", 3, index % 65536) + payload

    @staticmethod
    def is_ack(data: bytes, wire: int) -> bool:
        return (len(data) >= 4 and struct.unpack("!H", data[:2])[0] == OP_ACK
                and struct.unpack("!H", data[2:4])[0] == wire)


def make_payload(size: int, ticket: str = "WO") -> bytes:
    out = bytearray()
    i = 0
    while len(out) < size:
        out.extend(f"[{i:06d}] boot log {ticket} {sha(str(i).encode())[:16]}\n".encode())
        i += 1
    return bytes(out[:size])


def blocks_of(payload: bytes):
    blocks = [payload[i:i + BLKSIZE] for i in range(0, len(payload), BLKSIZE)]
    if not blocks or len(payload) % BLKSIZE == 0:
        blocks.append(b"")
    return blocks


async def start_service(tmpdir, timeout=0.6, retries=3, upload_timeout=None,
                        upload_retries=None):
    image = generate_image(4 * BLKSIZE, "test-seed")
    service = TftpServerService(
        image, bind_host="127.0.0.1", port=0, timeout=timeout, max_retries=retries,
        archive_dir=os.path.join(tmpdir, "archive"),
        staging_dir=os.path.join(tmpdir, "staging"),
        upload_timeout=upload_timeout or timeout,
        upload_max_retries=upload_retries if upload_retries is not None else retries,
    )
    await service.start()
    return service


def client_cfg(service, ticket, **kw):
    kw.setdefault("host", "127.0.0.1")
    kw.setdefault("port", service.port)
    kw.setdefault("timeout", 0.6)
    kw.setdefault("max_retries", 8)
    return ClientConfig(filename=ticket + ".log", **kw)


async def wait_history(service, ticket):
    for _ in range(60):
        if any(s["work_order"] == ticket for s in service.store.history):
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"no history entry for {ticket}")


async def scenario_basic_upload(service, tmpdir):
    print("== upload: short tail, exact-multiple empty terminator, zero bytes ==")
    payload = make_payload(1200, "WO-BASIC")
    res = await TftpClient(client_cfg(service, "WO-BASIC", seed=1)).upload(payload)
    check("upload ok", res.ok, res.error or "")
    path = os.path.join(tmpdir, "archive", "WO-BASIC.log")
    check("log visible in archive immediately after final ACK", os.path.isfile(path))
    blob = open(path, "rb").read()
    check("archived bytes match payload", blob == payload and sha(blob) == sha(payload))
    check("3 blocks on the wire (512+512+176)", res.stats["blocks"] == 3,
          str(res.stats["blocks"]))
    check("one server transfer port only", len(res.stats["server_ports"]) == 1,
          str(res.stats["server_ports"]))
    await asyncio.sleep(0.05)
    check("work order reservation released", "WO-BASIC" not in service.store.active)
    check("staging directory empty", os.listdir(os.path.join(tmpdir, "staging")) == [])

    await wait_history(service, "WO-BASIC")
    h = [s for s in service.store.history if s["work_order"] == "WO-BASIC"][0]
    check("server sha256 recorded", h["sha256"] == sha(payload))

    exact = make_payload(2 * BLKSIZE, "WO-EXACT")
    res2 = await TftpClient(client_cfg(service, "WO-EXACT", seed=2)).upload(exact)
    check("exact-multiple upload ok", res2.ok, res2.error or "")
    check("empty terminator block sent (3 blocks for 1024 bytes)",
          res2.stats["blocks"] == 3, str(res2.stats["blocks"]))
    check("archived 1024 bytes", os.path.getsize(
        os.path.join(tmpdir, "archive", "WO-EXACT.log")) == 1024)

    res3 = await TftpClient(client_cfg(service, "WO-EMPTY", seed=3)).upload(b"")
    check("zero-byte upload ok (single empty DATA)", res3.ok, res3.error or "")
    check("one empty block", res3.stats["blocks"] == 1 and res3.stats["bytes_sent"] == 0)
    check("empty file archived", os.path.getsize(
        os.path.join(tmpdir, "archive", "WO-EMPTY.log")) == 0)


async def scenario_archived_no_overwrite(service, tmpdir):
    print("== upload: archived log cannot be overwritten, bad names rejected ==")
    payload = make_payload(300, "WO-KEEP")
    res = await TftpClient(client_cfg(service, "WO-KEEP", seed=4)).upload(payload)
    check("first upload ok", res.ok, res.error or "")
    before = open(os.path.join(tmpdir, "archive", "WO-KEEP.log"), "rb").read()

    res2 = await TftpClient(client_cfg(service, "WO-KEEP", seed=5,
                                       timeout=0.4, max_retries=2)).upload(b"DIFFERENT")
    check("second WRQ refused", not res2.ok, res2.error or "")
    check("refusal is TFTP ERROR 6 (file exists)",
          res2.error and "ERROR 6" in res2.error, res2.error or "")
    after = open(os.path.join(tmpdir, "archive", "WO-KEEP.log"), "rb").read()
    check("archived bytes untouched", before == after and after == payload)
    check("no staging left behind", os.listdir(os.path.join(tmpdir, "staging")) == [])

    async with RawUploader(("127.0.0.1", service.port), "../escape.log") as raw:
        raw.send(build_wrq("../escape.log"))
        kind, code, msg, _ = await raw.wait_ack0()
        check("path traversal rejected with ERROR 4", kind == "ERROR" and code == 4,
              f"{kind} {code} {msg}")
    async with RawUploader(("127.0.0.1", service.port), "nodir/wo.log") as raw:
        raw.send(build_wrq("nodir/wo.log"))
        kind, code, msg, _ = await raw.wait_ack0()
        check("subdir filename rejected with ERROR 4", kind == "ERROR" and code == 4,
              f"{kind} {code} {msg}")


async def scenario_concurrent_same_ticket(service, tmpdir):
    print("== upload: same work order concurrent WRQ -> only one accepted ==")
    payload = make_payload(700, "WO-CONC")

    async with RawUploader(("127.0.0.1", service.port), "WO-CONC.log") as c1, \
            RawUploader(("127.0.0.1", service.port), "WO-CONC.log") as c2:
        c1.send(build_wrq("WO-CONC.log"))
        kind1, code1, msg1, _ = await c1.wait_ack0()
        c2.send(build_wrq("WO-CONC.log"))
        kind2, code2, msg2, _ = await c2.wait_ack0()
        check("first WRQ gets ACK 0", kind1 == "ACK0", f"{kind1} {code1} {msg1}")
        check("concurrent same-ticket WRQ gets an ERROR", kind2 == "ERROR",
              f"{kind2} {code2} {msg2}")
        check("ERROR code is 2 (access violation / in progress)", code2 == 2,
              str(code2))
        check("rejected client never got a transfer port (answered from 69)",
              c2.packets[0][1][1] == service.port)

        blocks = blocks_of(payload)
        for i, blk in enumerate(blocks, 1):
            c1.send(c1.data_pkt(i, blk), ("127.0.0.1", c1.transfer_port))
            data, addr = await c1.recv()
            check(f"winner block {i} ACKed from its transfer port",
                  c1.is_ack(data, i % 65536) and addr[1] == c1.transfer_port)
        path = os.path.join(tmpdir, "archive", "WO-CONC.log")
        check("exactly one archived file", os.path.isfile(path))
        check("archived content is the winner's payload",
              open(path, "rb").read() == payload)
        await wait_history(service, "WO-CONC")
        check("only one upload session ever recorded for the ticket",
              sum(1 for s in service.store.history if s["work_order"] == "WO-CONC") == 1,
              str([s["work_order"] for s in service.store.history]))


async def scenario_final_ack_lost(service, tmpdir):
    print("== upload: final ACK lost -> retransmit from same port, write once ==")
    payload = make_payload(600, "WO-FINAL")
    async with RawUploader(("127.0.0.1", service.port), "WO-FINAL.log",
                           timeout=3.0) as raw:
        raw.send(build_wrq("WO-FINAL.log"))
        kind, _, _, _ = await raw.wait_ack0()
        check("WRQ accepted", kind == "ACK0")
        blocks = blocks_of(payload)
        for i, blk in enumerate(blocks[:-1], 1):
            raw.send(raw.data_pkt(i, blk), ("127.0.0.1", raw.transfer_port))
            data, addr = await raw.recv()
            check(f"block {i} ACKed", raw.is_ack(data, i % 65536))
        final_index = len(blocks)
        final_wire = final_index % 65536
        # send the final block but swallow the first final ACK
        raw.send(raw.data_pkt(final_index, blocks[-1]), ("127.0.0.1", raw.transfer_port))
        data1, addr1 = await raw.recv()
        check("first final ACK received", raw.is_ack(data1, final_wire))
        first_port = addr1[1]
        # the server times out and retransmits the final ACK on its own
        data2, addr2 = await raw.recv()
        check("final ACK retransmitted within the retry window",
              raw.is_ack(data2, final_wire), f"got {data2[:4].hex()}")
        check("retransmission comes from the ORIGINAL transfer port",
              addr2[1] == first_port == raw.transfer_port,
              f"{first_port} vs {addr2[1]} vs {raw.transfer_port}")

    path = os.path.join(tmpdir, "archive", "WO-FINAL.log")
    check("log archived despite the lost ACK", os.path.isfile(path))
    check("archived content intact", open(path, "rb").read() == payload)
    await wait_history(service, "WO-FINAL")
    hist = [s for s in service.store.history if s["work_order"] == "WO-FINAL"][0]
    check("final ACK sent at least twice", hist["final_ack_sent"] >= 2,
          str(hist["final_ack_sent"]))
    check("archive written exactly once (one history entry, correct size)",
          hist["result"].startswith("completed") and hist["bytes_written"] == len(payload))
    check("no staging residue", os.listdir(os.path.join(tmpdir, "staging")) == [])


async def scenario_duplicate_final_data(service, tmpdir):
    print("== upload: client re-sends the final DATA -> duplicate re-ACK, write once ==")
    async with RawUploader(("127.0.0.1", service.port), "WO-DUP.log", timeout=3.0) as raw:
        raw.send(build_wrq("WO-DUP.log"))
        kind, _, _, _ = await raw.wait_ack0()
        check("WRQ accepted", kind == "ACK0")
        body = b"short final block"
        pkt = raw.data_pkt(1, body)
        raw.send(pkt, ("127.0.0.1", raw.transfer_port))
        d1, a1 = await raw.recv()
        check("final block ACKed", raw.is_ack(d1, 1))
        raw.send(pkt, ("127.0.0.1", raw.transfer_port))  # retransmitted final DATA
        d2, a2 = await raw.recv()
        check("duplicate final DATA re-ACKed", raw.is_ack(d2, 1), d2[:4].hex())
        check("duplicate answered from the ORIGINAL transfer port",
              a2[1] == a1[1] == raw.transfer_port)
    path = os.path.join(tmpdir, "archive", "WO-DUP.log")
    check("archive holds exactly one copy", open(path, "rb").read() == body)
    await wait_history(service, "WO-DUP")
    h = [s for s in service.store.history if s["work_order"] == "WO-DUP"][0]
    check("duplicate counted, in-order block count unchanged",
          h["duplicate_data"] == 1 and h["data_blocks"] == 1, str(h))


async def scenario_timeout_cleanup(service, tmpdir):
    print("== upload: interrupted transfer times out -> partial removed, ticket reusable ==")
    async with RawUploader(("127.0.0.1", service.port), "WO-IDLE.log",
                           timeout=3.0) as raw:
        raw.send(build_wrq("WO-IDLE.log"))
        kind, _, _, _ = await raw.wait_ack0()
        check("WRQ accepted", kind == "ACK0")
        raw.send(raw.data_pkt(1, b"x" * BLKSIZE), ("127.0.0.1", raw.transfer_port))
        data, _ = await raw.recv()
        check("block 1 ACKed", raw.is_ack(data, 1))
        check("reservation active while uploading", "WO-IDLE" in service.store.active)
        staging = os.path.join(tmpdir, "staging", "WO-IDLE.part")
        check("staging file exists mid-transfer", os.path.isfile(staging))
        # go silent: server must exhaust retries, remove the partial, free ticket
        deadline = 4.0
        while "WO-IDLE" in service.store.active and deadline > 0:
            await asyncio.sleep(0.1)
            deadline -= 0.1
        check("session unregistered after timeout", "WO-IDLE" not in service.store.active)
        check("partial staging file deleted", not os.path.exists(staging))
        check("nothing leaked into the archive",
              not os.path.exists(os.path.join(tmpdir, "archive", "WO-IDLE.log")))
        await wait_history(service, "WO-IDLE")
        hist = [s for s in service.store.history if s["work_order"] == "WO-IDLE"][-1]
        check("history marks an aborted timeout", "aborted" in hist["result"], hist["result"])

    payload = make_payload(200, "WO-IDLE")
    res = await TftpClient(client_cfg(service, "WO-IDLE", seed=6)).upload(payload)
    check("work order reusable after cleanup", res.ok, res.error or "")
    check("reused ticket archives the new content",
          open(os.path.join(tmpdir, "archive", "WO-IDLE.log"), "rb").read() == payload)


async def scenario_write_failure(service, tmpdir, monkeypatch_target):
    print("== upload: write/commit failure -> TFTP ERROR, no archive, ticket reusable ==")
    real_replace = os.replace
    failures = {"n": 0}

    def boom(src, dst):
        failures["n"] += 1
        raise OSError("simulated disk failure on rename")

    os.replace = boom
    try:
        async with RawUploader(("127.0.0.1", service.port), "WO-DISK.log",
                               timeout=2.0) as raw:
            raw.send(build_wrq("WO-DISK.log"))
            kind, _, _, _ = await raw.wait_ack0()
            check("WRQ accepted before the failure", kind == "ACK0")
            raw.send(raw.data_pkt(1, b"short final block"),
                     ("127.0.0.1", raw.transfer_port))
            data, addr = await raw.recv()
            op = struct.unpack("!H", data[:2])[0]
            check("client receives a TFTP ERROR on write failure", op == OP_ERROR,
                  f"op={op}")
            if op == OP_ERROR:
                code = struct.unpack("!H", data[2:4])[0]
                check("error code is 3 (disk full / write failure)", code == 3, str(code))
    finally:
        os.replace = real_replace
    check("failure injected exactly at commit", failures["n"] == 1, str(failures["n"]))
    check("no archive produced",
          not os.path.exists(os.path.join(tmpdir, "archive", "WO-DISK.log")))
    check("staging cleaned up", not os.path.exists(
        os.path.join(tmpdir, "staging", "WO-DISK.part")))
    await asyncio.sleep(0.05)
    check("ticket released", "WO-DISK" not in service.store.active)

    payload = make_payload(250, "WO-DISK")
    res = await TftpClient(client_cfg(service, "WO-DISK", seed=7)).upload(payload)
    check("work order reusable after the failure", res.ok, res.error or "")
    check("new upload archives correctly",
          open(os.path.join(tmpdir, "archive", "WO-DISK.log"), "rb").read() == payload)


async def scenario_data_loss(service, tmpdir):
    print("== upload: 25% outbound DATA loss -> server re-ACKs, transfer completes ==")
    payload = make_payload(5000, "WO-LOSS")
    res = await TftpClient(client_cfg(service, "WO-LOSS", data_loss=0.25,
                                      seed=42, max_retries=15)).upload(payload)
    check("upload ok despite DATA loss", res.ok, res.error or "")
    check("some DATA was actually dropped", res.stats["data_drops"] > 0,
          str(res.stats["data_drops"]))
    check("retries happened", res.stats["timeouts"] > 0, str(res.stats["timeouts"]))
    blob = open(os.path.join(tmpdir, "archive", "WO-LOSS.log"), "rb").read()
    check("archived bytes match despite loss", blob == payload)


async def scenario_api_surface(service, tmpdir):
    print("== api: /upload, /logs, info and download endpoints over ASGI ==")
    os.environ["UPLOAD_ARCHIVE_DIR"] = os.path.join(tmpdir, "archive")
    os.environ["TFTP_SERVER_HOST"] = "127.0.0.1"
    os.environ["TFTP_SERVER_PORT"] = str(service.port)
    import importlib
    import api.main as api_main
    importlib.reload(api_main)  # pick up the env-driven paths/ports
    import httpx
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api_main.app),
                                 base_url="http://test") as ac:
        r = await ac.post("/upload", params={"work_order": "WO-API", "payload_size": 1300})
        check("POST /upload 200", r.status_code == 200, str(r.status_code))
        body = r.json()
        check("upload ok and archive matches", body["ok"] and body["archive_match"],
              str(body.get("error")))
        check("archived size reported", body["archived_size_bytes"] == 1300)

        r2 = await ac.post("/upload", params={"work_order": "WO-API", "payload_size": 10})
        check("overwrite refused through the API", r2.status_code == 200
              and not r2.json()["ok"] and "ERROR 6" in (r2.json()["error"] or ""),
              str(r2.json().get("error")))

        r3 = await ac.get("/logs")
        names = [e["work_order"] for e in r3.json()["logs"]]
        check("GET /logs lists archived tickets", "WO-API" in names, str(names))
        check("all listed entries are real .log archives",
              all(e["filename"].endswith(".log") for e in r3.json()["logs"]))

        r4 = await ac.get("/logs/WO-API/info")
        check("GET info 200 with sha256", r4.status_code == 200
              and r4.json()["sha256"] == body["archived_sha256"])
        r5 = await ac.get("/logs/WO-API/download")
        check("GET download returns the bytes",
              r5.status_code == 200 and hashlib.sha256(r5.content).hexdigest()
              == body["archived_sha256"])
        r6 = await ac.get("/logs/NO-SUCH/info")
        check("missing ticket -> 404", r6.status_code == 404)
        r7 = await ac.post("/upload", params={"work_order": "bad/name", "payload_size": 10})
        check("bad work order -> 400", r7.status_code == 400)


async def amain():
    tmpdir = tempfile.mkdtemp(prefix="tftp-upload-")
    print(f"temporary data dir: {tmpdir}")
    try:
        # short timeouts make the timeout drills fast
        service = await start_service(tmpdir, timeout=0.6, retries=3,
                                      upload_timeout=0.6, upload_retries=3)
        try:
            await scenario_basic_upload(service, tmpdir)
            await scenario_archived_no_overwrite(service, tmpdir)
            await scenario_concurrent_same_ticket(service, tmpdir)
            await scenario_final_ack_lost(service, tmpdir)
            await scenario_duplicate_final_data(service, tmpdir)
            await scenario_timeout_cleanup(service, tmpdir)
            await scenario_write_failure(service, tmpdir, os.path.join(tmpdir, "x"))
            await scenario_data_loss(service, tmpdir)
            await scenario_api_surface(service, tmpdir)
        finally:
            await service.stop()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    failed = [name for name, ok in RESULTS if not ok]
    print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} checks passed")
    if failed:
        print("FAILED:", *failed, sep="\n  - ")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(amain()))
