#!/usr/bin/env bash
# Local throughput benchmark for pcurl.
#
# Serves a generated file over a small range-capable local HTTP server and times
# pcurl at several connection counts, reporting wall time and throughput.
# Intended as a rough, repeatable signal for tuning -c / -s, not a precise
# microbenchmark.
#
# Usage: scripts/bench.sh [SIZE_MB] [PORT]
set -euo pipefail

SIZE_MB="${1:-256}"
PORT="${2:-8088}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/target/release/pcurl"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"; [ -n "${SRV_PID:-}" ] && kill "$SRV_PID" 2>/dev/null || true' EXIT

if [ ! -x "$BIN" ]; then
  echo "[*] building release binary"
  (cd "$ROOT" && cargo build --release)
fi

echo "[*] generating ${SIZE_MB} MiB test file"
head -c "$((SIZE_MB * 1024 * 1024))" /dev/urandom > "$WORK/data.bin"

# The standard library http.server does not honour Range, so use a tiny
# range-capable handler. Threaded so parallel connections are served at once.
echo "[*] starting range-capable server on 127.0.0.1:${PORT}"
DATA_FILE="$WORK/data.bin" python3 - "$PORT" <<'PY' &
import os, sys
from http.server import BaseHTTPRequestHandler
from socketserver import ThreadingTCPServer

PATH = os.environ["DATA_FILE"]
SIZE = os.path.getsize(PATH)

class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *a): pass
    def do_GET(self):
        rng = self.headers.get("Range")
        with open(PATH, "rb") as f:
            if rng and rng.startswith("bytes="):
                s, _, e = rng[6:].partition("-")
                start = int(s) if s else 0
                end = int(e) if e else SIZE - 1
                end = min(end, SIZE - 1)
                length = end - start + 1
                f.seek(start)
                body = f.read(length)
                self.send_response(206)
                self.send_header("Content-Range", f"bytes {start}-{end}/{SIZE}")
                self.send_header("Accept-Ranges", "bytes")
                self.send_header("Content-Length", str(length))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(200)
                self.send_header("Accept-Ranges", "bytes")
                self.send_header("Content-Length", str(SIZE))
                self.end_headers()
                self.wfile.write(f.read())

ThreadingTCPServer.allow_reuse_address = True
ThreadingTCPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
PY
SRV_PID=$!
sleep 1

URL="http://127.0.0.1:${PORT}/data.bin"

run() {
  local conns="$1" chunk="$2"
  local start end secs mbps
  start=$(date +%s.%N)
  "$BIN" -c "$conns" -s "$chunk" -q "$URL" > /dev/null
  end=$(date +%s.%N)
  secs=$(awk "BEGIN{print $end-$start}")
  mbps=$(awk "BEGIN{printf \"%.1f\", $SIZE_MB/$secs}")
  printf "  -c %-3s -s %-4s : %6.2fs  (%s MiB/s)\n" "$conns" "$chunk" "$secs" "$mbps"
}

echo "[*] downloading ${SIZE_MB} MiB at varying connection counts"
run 1 8M
run 4 8M
run 8 4M
run 16 4M
run 32 2M
echo "[OK] done"
