#!/bin/bash
# Re-fetch provider playlist and regenerate full.m3u (rewritten live URLs).
# Runs daily via /etc/cron.d/iptv-remux.
set -euo pipefail
DIR=/root/iptv-remux
set -a; . "$DIR/iptv-remux.env"; set +a

curl -sS --max-time 180 -o "$DIR/provider.m3u.tmp" "$IPTV_PLAYLIST_URL"
mv "$DIR/provider.m3u.tmp" "$DIR/provider.m3u"
python3 "$DIR/rewrite.py"
