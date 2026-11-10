# IPTV remux proxy — webOS 26 workaround

## Why this exists
webOS 26 (firmware 43.21.x) has a regression in its live-stream media pipeline:
live IPTV (MPEG-TS and HLS) plays 1-3s then freezes/buffers/loops, on ALL IPTV
apps. VOD is unaffected. LG has no fix yet (Nov 2026).
Refs:
- https://forum.webostv.developer.lge.com/t/webos-26-firmware-43-21-71-live-stream-hls-mpeg-ts-playback-loop-buffer-underrun-bug-on-c3-oled/28358
- https://forum.webostv.developer.lge.com/t/ebos-26-live-iptv-streams-freeze-after-1-2-seconds-on-oled65c6-all-apps-ts-hls/28471/1
- https://www.digital-eliteboard.com/threads/nach-update-vom-lg-web-frieren-iptv-streams-ein.550645/

On top of the firmware bug, the provider stream itself drops its HTTP
connection every ~30-90s ("Stream ends prematurely"), with corrupt packets and
timestamp discontinuities at each reconnect. The TV's player rides the HLS
playlist tail with ~zero prefetch (polls ~3.6x/sec), so ANY upstream hiccup
reaches the screen.

Workaround: a timeshifting re-encode proxy on PVE.

## Architecture
```
provider live TS --(python downloader, VLC UA, backoff)--> ~35s RAM ring
    buffer --(pacer thread, locked ~1x realtime feed)--> ffmpeg stdin
    --(deinterlace 1080i->1080p50, VAAPI h264 encode)
    --> HLS in ch/<id>/g-<gen>/ --> :8877 --> Hot IPTV on TV
```
- RAM backlog (TARGET_BACKLOG=30MB ~35s) absorbs provider drops/jitter:
  pacer feeds ffmpeg at the smoothed realtime rate — output NEVER races
  (faster output slides the HLS window out from under the tail-riding
  player) and never starves while backlog lasts.
- Generation isolation: each pipeline writes to `ch/<id>/g-<gen>/` and the
  served playlist rewrites segment URIs as `g-<gen>/segNNNNN.ts`. Restarts
  therefore never delete or renumber segments an in-flight player uses;
  old generations are reaped after 60s.
- ONLY `index.m3u8` requests spawn/restart pipelines — segment requests
  just serve files (a stale request can never wipe a live pipeline).
- `kill` is non-blocking (SIGKILL only, no pipe close, never under lock).
- LAN-only: requests from non-private IPs get 403.
- `proxy.py` — HTTP server on 0.0.0.0:8877 (stdlib only).
  - `GET /ch/<id>/index.m3u8` -> spawn pipeline, wait <=25s, serve playlist.
  - `GET /ch/<id>/g-<gen>/segNNNNN.ts` -> serve segment.
  - `GET /full.m3u` -> rewritten playlist.
  - Pipelines die after 45s idle; max 3 concurrent (dead-first LRU evict).
- `rewrite.py` — regenerates `full.m3u` from `provider.m3u`
  (only live `user/pass/<id>` URLs are rewritten; VOD stays direct).
- `refresh-playlist.sh` — fetches provider playlist + runs rewrite.py.
  Daily at 06:15 via `/etc/cron.d/iptv-remux`.
- `iptv-remux.service` — systemd unit, auto-starts on boot.
- `ch/<id>/g-<gen>/` — per-generation dirs (segments + ff.log).
- ffmpeg: static BtbN build at `/root/ffmpeg-master-latest-linux64-gpl/bin/ffmpeg`.

## Hot IPTV setup
Add playlist URL in the app (or its web portal):
```
http://192.168.1.42:8877/full.m3u
```

## Provider credentials
Live in `iptv-remux.env` next to proxy.py on PVE (gitignored — see
`iptv-remux.env.example`). Loaded by systemd (`EnvironmentFile=`) and
`refresh-playlist.sh`; never commit the real file.
IMPORTANT: the panel's Cloudflare blocks generic clients — urllib MUST send a
real User-Agent (proxy uses `VLC/3.0.20`). Bare `urlopen` gets 403.

## Operations
- Status:   `systemctl status iptv-remux`
- Restart:  `systemctl restart iptv-remux`
- Logs:     per-channel `/root/iptv-remux/ch/<id>/g-<gen>/ff.log`,
  requests `/root/iptv-remux/serve.log`
- Playlist: `bash /root/iptv-remux/refresh-playlist.sh` (manual refresh)
- Buffer mode toggle (live, no restart, persists across restarts via
  `buffer.mode` file):
  - deep:    `curl http://192.168.1.42:8877/buffer/deep`
             (keep 25MB ≈ 28s, cap 140MB ≈ ~2.5min cushion; tune-in same,
             playback lags live as cushion grows)
  - normal:  `curl http://192.168.1.42:8877/buffer/normal` (keep 8MB ≈ 9s)
  - status:  `curl http://192.168.1.42:8877/buffer`
- Disable:  `systemctl disable --now iptv-remux` + re-add the provider's
  original playlist URL in Hot IPTV.

## Tuning (edit proxy.py)
- Quality: `-rc_mode CQP -qp 22` (VAAPI h264, lower qp = better/bigger)
- Field order: `-vf bwdif=mode=send_field:parity=tff` — source claims top-first;
  flip to `bff` if motion ever ping-pongs again.
- `PREFILL_NORMAL`/`PREFILL_DEEP` — timeshift depth per mode (~9s / ~28s).
  Deep mode trades tune-in speed for drop coverage; toggle at runtime via
  `/buffer/deep|normal` (no restart needed).
- `MAX_BACKLOG`/`MAX_BACKLOG_DEEP` — RAM caps (70MB / 140MB).
- `MAX_PIPELINES`, `IDLE_KILL` at top of file.

## History / symptoms timeline (2026-11-10)
1. remux `-c copy` + genpts -> played, but ~5s loop-back every 15-20s
2. + `-use_wallclock_as_timestamps` -> 1-2s micro-stutters (bursty arrival ts)
3. full re-encode (deint + bf 0) -> stable, but future/past judder (bwdif
   parity=auto flipping on mislabeled interlace flags)
4. `parity=tff` pinned -> clean picture
5. provider drops still visible (~1-3s pauses every minute-ish) ->
   withhold-buffer v1 (served tail 8 segs behind) -> player starves on slow
   stretches (it rides the tail with zero prefetch; polls 3.6x/sec)
6. timeshift v1: half-speed warmup feed -> connecting loops; then
   burst-inflated rate estimate -> 3x-realtime output -> HLS window slid
   out from under the player -> more connecting loops
7. timeshift v3 (current): locked ~1x feed (bursts fill backlog, stalls
   drain it), generation-isolated segment dirs so restarts never collide
   with in-flight players, seg requests can't respawn pipelines,
   non-blocking teardown, downloader backoff, LAN-only.
