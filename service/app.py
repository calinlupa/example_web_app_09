"""The checkout service: `/health` and `/metrics`, with a failure that comes from this file.

Standard library only. A built-in load generator offers OFFERED_RPS checkout requests a second to a pool of POOL_SIZE
workers (a small queue in front). Each request has to wait for the payment provider's answer, and how it waits is the
whole story of the demo:

- "async": the provider calls our webhook later; a worker is busy for ~50 ms and sleeps meanwhile.
- "sync": the request waits for the answer inside the worker, polling in a tight loop (`while not done: pass`); a worker
  is busy for ~3.4 s. The pool saturates, the queue overflows, requests are rejected, latency climbs and the machine's
  CPU goes to 100 percent: the error rate and latency reported below are measured from those requests, and the CPU
  burn is real.

Changing CONFIRMATION_MODE is what the demo's bad deploy is. The version reported is the git commit the service runs
(`git rev-parse --short HEAD`). The service is stateless: orders live in an external database, so replacing the machine
with a known-good image loses nothing.

    python3 app.py [port]
"""

from __future__ import annotations

import json
import queue
import subprocess
import sys
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

CONFIRMATION_MODE = "sync"  # wait for the provider answer inside the request (about 3.4 s)
POOL_SIZE = 8  # workers
OFFERED_RPS = 4  # checkout requests a second
QUEUE_LIMIT = 4  # requests waiting for a worker; beyond that they are rejected
HOLD_SECONDS = {"async": 0.05, "sync": 3.4}[CONFIRMATION_MODE]  # how long a request holds a worker
WINDOW_SECONDS = 30  # what /health looks back over

HISTORY = Path("/var/lib/checkout/history.jsonl")
jobs: queue.Queue[float] = queue.Queue(maxsize=QUEUE_LIMIT)
events: deque[tuple[float, bool, float]] = deque()  # (finished at, served, latency in ms)
lock = threading.Lock()
STOP = threading.Event()  # set to end the workers and the load generator (tests; the service itself runs until killed)


def version() -> str:
    try:
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=Path(__file__).parent, capture_output=True,
                              text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


VERSION = version()


def record_event(served: bool, latency_ms: float) -> None:
    now = time.time()
    with lock:
        events.append((now, served, latency_ms))
        while events and events[0][0] < now - 120:
            events.popleft()


def worker() -> None:
    while not STOP.is_set():
        try:
            arrived = jobs.get(timeout=0.2)
        except queue.Empty:
            continue
        if CONFIRMATION_MODE == "async":
            time.sleep(HOLD_SECONDS)  # the provider answers through our webhook later
        else:
            end = time.monotonic() + HOLD_SECONDS
            while time.monotonic() < end and not STOP.is_set():  # wait for the provider's answer, polling in a tight loop
                pass
        record_event(True, (time.time() - arrived) * 1000)


def load() -> None:
    while not STOP.is_set():
        try:
            jobs.put_nowait(time.time())
        except queue.Full:
            record_event(False, 0.0)  # no worker free and the queue is full: the request is rejected
        STOP.wait(1 / OFFERED_RPS)


def state(now: float, window: float = WINDOW_SECONDS) -> dict:
    """Error rate and p95 latency over the last `window` seconds, measured from the requests."""
    with lock:
        recent = [e for e in events if e[0] >= now - window]
    served = sorted(e[2] for e in recent if e[1])
    errors = sum(1 for e in recent if not e[1])
    p95 = served[min(int(len(served) * 0.95), len(served) - 1)] if served else 0.0
    return {"version": VERSION, "error_rate": round(errors / len(recent), 3) if recent else 0.0, "p95_ms": round(p95)}


def record(now: float) -> None:
    """One point a minute for /metrics."""
    minute = int(now // 60)
    HISTORY.parent.mkdir(parents=True, exist_ok=True)
    last = HISTORY.read_text().splitlines()[-1:] if HISTORY.exists() else []
    if last and json.loads(last[0])["minute"] == minute:
        return
    point = {"minute": minute, "t": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(minute * 60)), **state(now, 60)}
    with HISTORY.open("a") as f:
        f.write(json.dumps(point) + "\n")


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        now = time.time()
        if self.path == "/health":
            current = state(now)
            body = {"status": "failing" if current["error_rate"] > 0.05 else "healthy", **current}
            code = 503 if body["status"] == "failing" else 200
        elif self.path.startswith("/metrics"):
            lines = HISTORY.read_text().splitlines()[-60:] if HISTORY.exists() else []
            body, code = [{k: v for k, v in json.loads(line).items() if k != "minute"} for line in lines], 200
        else:
            body, code = {"error": "not found"}, 404
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args: object) -> None:
        pass


def keep_recording() -> None:
    while not STOP.is_set():
        record(time.time())
        STOP.wait(15)


def start_background() -> None:
    sys.setswitchinterval(0.002)  # keep the HTTP handler responsive while workers spin
    for _ in range(POOL_SIZE):
        threading.Thread(target=worker, daemon=True).start()
    threading.Thread(target=load, daemon=True).start()
    threading.Thread(target=keep_recording, daemon=True).start()


if __name__ == "__main__":
    start_background()
    ThreadingHTTPServer(("0.0.0.0", int(sys.argv[1]) if len(sys.argv) > 1 else 8080), Handler).serve_forever()
