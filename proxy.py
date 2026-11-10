#!/usr/bin/env python3
"""On-demand IPTV live-stream remuxer for webOS 26 workaround.

Architecture (v3 - timeshift + generation-isolated pipelines):
  provider TS --(downloader, VLC UA, backoff)--> ~35s RAM ring buffer
    --(pacer, locked ~1x realtime feed)--> ffmpeg stdin
    --(deinterlace 1080i->1080p50, VAAPI h264)--> HLS in ch/<id>/g-<gen>/
    --> :8877 playlist rewritten with generation-scoped segment URIs --> TV

Key invariants:
  - Segment files are immutable per generation; a restart never deletes or
    renumbers segments an in-flight player still references (old gens are
    reaped after GEN_GRACE).
  - Only index.m3u8 requests spawn/restart pipelines. Segment requests only
    serve files — a stale request can never wipe a live pipeline.
  - Pacer feeds at the smoothed realtime rate; provider bursts grow the RAM
    backlog, provider stalls drain it — output pace never leaves ~1x.
  - kill() never blocks (no pipe close, SIGKILL only) and never runs
    under the request lock.
"""
import http.server, socketserver, threading, subprocess, os, time, re
import urllib.request, ipaddress, shutil
from collections import deque

BASE = "/root/iptv-remux"
CHROOT = os.path.join(BASE, "ch")
PANEL = os.environ.get("IPTV_PANEL_TEMPLATE")
if not PANEL:
    raise SystemExit("IPTV_PANEL_TEMPLATE env var not set "
                     "(see iptv-remux.env.example)")
UA = {"User-Agent": "VLC/3.0.20 LibVLC/3.0.20"}
FFMPEG = "/root/ffmpeg-master-latest-linux64-gpl/bin/ffmpeg"
PORT = 8877
IDLE_KILL = 45
MAX_PIPELINES = 3
LIST_SIZE = 20

TARGET_BACKLOG = 30 * 1024 * 1024   # ~35s at ~7Mbps
MAX_BACKLOG = 70 * 1024 * 1024      # hard cap; oldest bytes dropped on overflow
MAX_BACKLOG_DEEP = 140 * 1024 * 1024
CHUNK = 64 * 1024
PREFILL_NORMAL = 8 * 1024 * 1024    # steady-state hold-back depth (~9s)
PREFILL_DEEP = 25 * 1024 * 1024     # deep-mode keep depth (~28s); /buffer/deep
PREFILL_MAX_SECS = 10.0             # cap on prefill wait (bounds tune-in delay)
cfg = {"deep": False}               # runtime buffer mode (bool flip is atomic)
MODE_FILE = os.path.join(BASE, "buffer.mode")
GEN_GRACE = 60.0                    # keep old-generation segments this long
RESYNC_GIVEUP = 6.0                 # max seconds to wait for the live edge

LAN = [ipaddress.ip_network(n) for n in
       ("192.168.0.0/16", "10.0.0.0/8", "172.16.0.0/12",
        "127.0.0.0/8", "169.254.0.0/16")]
SEG_RE = re.compile(r"^seg\d+\.ts$")
GEN_RE = re.compile(r"^g-[0-9a-f]+$")

lock = threading.Lock()
pipes = {}  # cid -> state dict


def ch_dir(cid):
    return os.path.join(CHROOT, cid)


def logline(msg):
    try:
        with open(os.path.join(BASE, "serve.log"), "a") as fh:
            fh.write("%s %s\n" % (time.strftime("%H:%M:%S"), msg))
    except OSError:
        pass


def buf_push(st, data):
    with st["buf_lock"]:
        st["buf"].append(data)
        st["buf_bytes"] += len(data)
        cap = MAX_BACKLOG_DEEP if cfg["deep"] else MAX_BACKLOG
        while st["buf_bytes"] > cap:
            st["buf_bytes"] -= len(st["buf"].popleft())


def trim_buf(st, keep):
    """Drop oldest chunks until backlog <= keep — skips the provider's
    replay/catch-up burst so output resumes near the live edge."""
    with st["buf_lock"]:
        while st["buf"] and st["buf_bytes"] > keep:
            st["buf_bytes"] -= len(st["buf"].popleft())


def drop_bytes(st, n):
    """Drop exactly n bytes from the buffer head."""
    with st["buf_lock"]:
        while n > 0 and st["buf"]:
            head = st["buf"][0]
            if len(head) <= n:
                n -= len(head)
                st["buf_bytes"] -= len(head)
                st["buf"].popleft()
            else:
                st["buf"][0] = head[n:]
                st["buf_bytes"] -= n
                n = 0


def deque_resync_off(st, hi):
    """Scan the whole backlog for the first PCR >= hi-2s — i.e. where live
    content resumes after a replay region. Returns byte offset to drop to
    reach it, or None if live hasn't landed yet."""
    off = 0
    with st["buf_lock"]:
        for ch in st["buf"]:
            for po, p in find_pcrs(ch):
                if p >= hi - 2 * 90000:
                    return off + po
            off += len(ch)
    return None


def buf_pop(st, n):
    with st["buf_lock"]:
        if not st["buf"]:
            return b""
        out = bytearray()
        while st["buf"] and len(out) < n:
            head = st["buf"][0]
            take = min(n - len(out), len(head))
            out += head[:take]
            st["buf_bytes"] -= take
            if take == len(head):
                st["buf"].popleft()
            else:
                st["buf"][0] = head[take:]
        return bytes(out)


def downloader(cid, st):
    """Pull provider TS into the ring buffer. Instant reconnect after a
    productive session; exponential backoff on repeated failures."""
    fails = 0
    while not st["stop"].is_set():
        got = 0
        try:
            req = urllib.request.Request(PANEL.format(cid), headers=UA)
            with urllib.request.urlopen(req, timeout=15) as resp:
                st["resync"] = True          # provider replays stale buffer on reconnect
                st["resync_t0"] = time.monotonic()
                while not st["stop"].is_set():
                    data = resp.read1(CHUNK)
                    if not data:
                        break
                    buf_push(st, data)
                    got += len(data)
        except Exception:
            pass
        fails = 0 if got > 4 * CHUNK else fails + 1
        st["stop"].wait(min(0.5 * fails, 8.0))


def find_pcrs(data):
    """Locate MPEG-TS packets in an arbitrary chunk and extract PCR values.
    Returns [(byte_offset, pcr_90kHz)]."""
    res = []
    n = len(data)
    i = 0
    while i + 564 <= n:
        if data[i] == 0x47 and data[i + 188] == 0x47 and data[i + 376] == 0x47:
            break
        i += 1
    while i + 188 <= n:
        if data[i] != 0x47:
            i += 1
            continue
        if (data[i + 3] >> 4) & 3 in (2, 3) and data[i + 4] >= 7 and data[i + 5] & 0x10:
            b = data[i + 6:i + 12]
            res.append((i, (b[0] << 25) | (b[1] << 17) | (b[2] << 9) |
                        (b[3] << 1) | (b[4] >> 7)))
        i += 188
    return res


def pacer(cid, st, proc):
    """Write buffered bytes to ffmpeg stdin. ffmpeg runs with -re, so it
    consumes input at the stream's own timestamp pace — the OS pipe's
    backpressure IS the pacing. Provider bursts inflate the backlog,
    stalls drain it; output can never race or lag the content clock.
    PCR watchdog: the provider replays ~seconds of stale content on every
    reconnect — a PCR backward jump marks it; we drop bytes until PCR
    resumes at the live edge instead of feeding the rewind to the TV."""
    t0 = time.monotonic()
    fed = False
    while not st["stop"].is_set() and proc.poll() is None:
        if not fed:
            with st["buf_lock"]:
                backlog = st["buf_bytes"]
            if (backlog < PREFILL_NORMAL
                    and time.monotonic() - t0 < PREFILL_MAX_SECS):
                time.sleep(0.2)
                continue
            trim_buf(st, PREFILL_DEEP if cfg["deep"] else PREFILL_NORMAL)
            fed = True
        data = buf_pop(st, CHUNK)
        if not data:
            # backlog exhausted — provider outlasted the cushion. Re-enter
            # prefill so the reconnect burst refills it (and its stale
            # replay gets trimmed) instead of leaking through.
            fed = False
            t0 = time.monotonic()
            time.sleep(0.1)
            continue
        pcrs = find_pcrs(data)
        hi = st["pcr_hi"]
        if (not st["resync"] and hi is not None
                and any(p < hi - 30 * 90000 for _o, p in pcrs)):
            # Mid-connection splice: provider rewound content inside the
            # same HTTP stream by >30s. Engage resync — scan the whole
            # backlog for where live resumes so the replay region drops in
            # one shot instead of starving ffmpeg while replay trickles in.
            st["resync"] = True
            st["resync_t0"] = time.monotonic()
            logline("ch%s rewind-detect" % cid)
        if st["resync"]:
            if hi is None:
                st["resync"] = False
            else:
                # push the popped chunk back so the deque scan sees it too
                with st["buf_lock"]:
                    st["buf"].appendleft(data)
                    st["buf_bytes"] += len(data)
                off = deque_resync_off(st, hi)
                if off is not None:
                    drop_bytes(st, off)
                    st["resync"] = False
                    logline("ch%s resync-cut %dB" % (cid, off))
                elif time.monotonic() - st["resync_t0"] > RESYNC_GIVEUP:
                    st["resync"] = False
                    st["pcr_hi"] = None        # adopt whatever epoch comes next
                    logline("ch%s resync-giveup" % cid)
                else:
                    time.sleep(0.1)
                continue
        for _o, p in pcrs:
            if st["pcr_hi"] is None or p > st["pcr_hi"]:
                st["pcr_hi"] = p
        if not data:
            continue
        try:
            proc.stdin.write(data)
            proc.stdin.flush()
        except (BrokenPipeError, OSError):
            break


def ensure(cid):
    """Return live pipeline state for cid; spawn or respawn if needed.
    Caller is do_GET on the playlist endpoint only."""
    with lock:
        p = pipes.get(cid)
        if p and p["proc"].poll() is None:
            p["last"] = time.monotonic()
            return p
        if p:
            stop_proc(p)
            del pipes[cid]
        if len(pipes) >= MAX_PIPELINES:
            dead = [k for k, v in pipes.items()
                    if v["proc"].poll() is not None]
            victim = min(dead or list(pipes), key=lambda k: pipes[k]["last"])
            stop_proc(pipes.pop(victim))
        gen = "g-%x" % time.monotonic_ns()
        d = os.path.join(ch_dir(cid), gen)
        os.makedirs(d, exist_ok=True)
        log = open(os.path.join(d, "ff.log"), "ab")
        proc = subprocess.Popen([
            "nice", "-n", "10", FFMPEG,
            "-hide_banner", "-loglevel", "warning",
            "-vaapi_device", "/dev/dri/renderD128",
            "-re", "-f", "mpegts", "-i", "pipe:0",
            "-vf", "bwdif=mode=send_field:parity=tff,format=nv12,hwupload",
            "-c:v", "h264_vaapi", "-rc_mode", "CQP", "-qp", "22",
            "-g", "60", "-bf", "0",
            "-c:a", "aac", "-b:a", "128k", "-ac", "2",
            "-max_muxing_queue_size", "2048", "-avoid_negative_ts", "make_zero",
            "-f", "hls", "-hls_time", "3", "-hls_list_size", str(LIST_SIZE),
            "-hls_flags", "delete_segments+program_date_time",
            "-hls_segment_filename", os.path.join(d, "seg%05d.ts"),
            os.path.join(d, "live.m3u8"),
        ], stdin=subprocess.PIPE, stdout=log, stderr=log)
        st = {"proc": proc, "last": time.monotonic(), "stop": threading.Event(),
              "gen": gen, "dir": d,
              "buf": deque(), "buf_bytes": 0, "buf_lock": threading.Lock(),
              "pcr_hi": None, "resync": False, "resync_t0": 0.0}
        if p:
            # Carry the undrained backlog + PCR epoch into the respawn, so a
            # crash/restart resumes mid-content instead of jumping the player
            # back to the live edge. Shares the old lock: a dying downloader
            # may still push one stray chunk.
            st["buf"], st["buf_lock"] = p["buf"], p["buf_lock"]
            with p["buf_lock"]:
                st["buf_bytes"] = p["buf_bytes"]
            st["pcr_hi"] = p["pcr_hi"]
        pipes[cid] = st
        threading.Thread(target=downloader, args=(cid, st), daemon=True).start()
        threading.Thread(target=pacer, args=(cid, st, proc), daemon=True).start()
        return st


def stop_proc(p):
    """Non-blocking teardown: signal workers, SIGKILL ffmpeg. Deliberately
    never closes proc.stdin (could block on a full pipe) and never deletes
    generation files (in-flight players may still need them)."""
    p["stop"].set()
    try:
        p["proc"].kill()
    except OSError:
        pass


def reaper():
    while True:
        time.sleep(10)
        now = time.monotonic()
        with lock:
            doomed = [c for c, p in pipes.items()
                      if now - p["last"] > IDLE_KILL
                      or (p["proc"].poll() is not None and now - p["last"] > 15)]
            for cid in doomed:
                stop_proc(pipes.pop(cid))
            active = {cid: p["gen"] for cid, p in pipes.items()}
        cleanup_gens(now, active)


def cleanup_gens(now, active):
    try:
        cids = os.listdir(CHROOT)
    except OSError:
        return
    for cid in cids:
        base = os.path.join(CHROOT, cid)
        if not os.path.isdir(base):
            continue
        for g in os.listdir(base):
            if not GEN_RE.match(g) or g == active.get(cid):
                continue
            gp = os.path.join(base, g)
            try:
                if now - os.path.getmtime(gp) > GEN_GRACE:
                    shutil.rmtree(gp, ignore_errors=True)
            except OSError:
                pass


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _lan_only(self):
        try:
            ip = ipaddress.ip_address(self.client_address[0])
            return any(ip in n for n in LAN)
        except ValueError:
            return False

    def do_HEAD(self):
        self._head = True
        try:
            self.do_GET()
        finally:
            self._head = False

    def do_GET(self):
        if not self._lan_only():
            return self.err(403)
        path = self.path.split("?")[0]
        if path == "/buffer" or path.startswith("/buffer/"):
            return self.buffer_ctl(path)
        if path in ("/full.m3u", "/iptv.m3u", "/playlist.m3u"):
            return self.serve_file(os.path.join(BASE, "full.m3u"),
                                   "audio/x-mpegurl")
        parts = [x for x in path.split("/") if x]
        if len(parts) >= 2 and parts[0] == "ch" and parts[1].isdigit():
            cid, fname = parts[1], parts[-1]
            if len(parts) == 3 and fname == "index.m3u8":
                st = ensure(cid)
                target = os.path.join(st["dir"], "live.m3u8")
                deadline = time.monotonic() + 25
                while time.monotonic() < deadline:
                    try:
                        if os.path.getsize(target) > 0:
                            break
                    except OSError:
                        pass
                    time.sleep(0.1)
                return self.serve_playlist(st)
            if len(parts) == 4 and GEN_RE.match(parts[2]) and SEG_RE.match(fname):
                with lock:
                    p = pipes.get(cid)
                    if p:
                        p["last"] = time.monotonic()
                return self.serve_seg(os.path.join(CHROOT, cid, parts[2]), fname)
            if len(parts) == 3 and SEG_RE.match(fname):
                with lock:
                    p = pipes.get(cid)
                    if p:
                        p["last"] = time.monotonic()
                if not p:
                    return self.err(404)
                return self.serve_seg(p["dir"], fname)
        return self.err(404)

    def buffer_ctl(self, path):
        mode = path.rsplit("/", 1)[-1]
        if mode == "deep":
            cfg["deep"] = True
        elif mode == "normal":
            cfg["deep"] = False
        elif path != "/buffer":
            return self.err(404)
        else:
            mode = None
        if mode:
            try:
                with open(MODE_FILE, "w") as fh:
                    fh.write(mode)
            except OSError:
                pass
        logline("buffer-mode %s" % ("deep" if cfg["deep"] else "normal"))
        return self._send(
            ("buffer=%s keep=%dMB cap=%dMB\n" % (
                "deep" if cfg["deep"] else "normal",
                (PREFILL_DEEP if cfg["deep"] else PREFILL_NORMAL) // 1048576,
                (MAX_BACKLOG_DEEP if cfg["deep"] else MAX_BACKLOG) // 1048576,
            )).encode(), "text/plain")

    def serve_playlist(self, st):
        """Serve live.m3u8 with segment URIs scoped to this generation, so
        in-flight players keep fetching consistent segments even if the
        pipeline later restarts."""
        fp = os.path.join(st["dir"], "live.m3u8")
        try:
            with open(fp) as fh:
                lines = fh.read().splitlines()
        except OSError:
            return self.err(404)
        nseg = 0
        for i, ln in enumerate(lines):
            s = ln.strip()
            if s and not s.startswith("#"):
                lines[i] = "%s/%s" % (st["gen"], s)
                nseg += 1
        logline("ch%s serve %s segs=%d" % (st["dir"].split("/")[-2],
                                           st["gen"], nseg))
        return self._send("\n".join(lines).encode(), "audio/x-mpegurl")

    def serve_seg(self, d, fname):
        fp = os.path.join(d, fname)
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            try:
                if os.path.getsize(fp) > 0:
                    return self.serve_file(fp, "video/mp2t")
            except OSError:
                pass
            time.sleep(0.1)
        logline("seg404 %s" % fp)
        return self.err(404)

    def serve_file(self, fp, ct):
        try:
            with open(fp, "rb") as fh:
                data = fh.read()
        except OSError:
            return self.err(404)
        return self._send(data, ct)

    def _send(self, data, ct):
        self.send_response(200)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        if not getattr(self, "_head", False):
            try:
                self.wfile.write(data)
            except OSError:
                pass

    def err(self, code):
        self.send_response(code)
        self.send_header("Content-Length", "0")
        self.end_headers()


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


if __name__ == "__main__":
    os.makedirs(CHROOT, exist_ok=True)
    try:
        cfg["deep"] = open(MODE_FILE).read().strip() == "deep"
    except OSError:
        pass
    threading.Thread(target=reaper, daemon=True).start()
    Server(("0.0.0.0", PORT), Handler).serve_forever()
