#!/bin/bash
# Re-fetch provider playlist and regenerate full.m3u (rewritten live URLs).
# Runs daily via /etc/cron.d/iptv-remux.
set -euo pipefail
DIR=/root/iptv-remux
set -a; . "$DIR/iptv-remux.env"; set +a

# --fail: a provider 4xx/5xx must not replace the playlist with an error body.
curl -fsS --max-time 180 -o "$DIR/provider.m3u.tmp" "$IPTV_PLAYLIST_URL"
head -1 "$DIR/provider.m3u.tmp" | grep -q "#EXTM3U" || {
    echo "provider response is not an m3u playlist; keeping old file" >&2
    exit 1
}
mv "$DIR/provider.m3u.tmp" "$DIR/provider.m3u"
python3 "$DIR/rewrite.py"
