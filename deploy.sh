#!/bin/bash
# Deploy proxy.py (+ optional extras) to PVE and verify.
# Usage: ./deploy.sh [all]   — "all" also syncs README/service/scripts/env
set -euo pipefail
PVE=root@192.168.1.42
DST=/root/iptv-remux

python3 -m py_compile proxy.py
ssh -o BatchMode=yes "$PVE" "mkdir -p $DST"
scp -o BatchMode=yes proxy.py "$PVE:$DST/proxy.py"

if [ "${1:-}" = "all" ]; then
    scp -o BatchMode=yes README.md iptv-remux.service refresh-playlist.sh rewrite.py "$PVE:$DST/"
    [ -f iptv-remux.env ] && scp -o BatchMode=yes iptv-remux.env "$PVE:$DST/iptv-remux.env"
    ssh -o BatchMode=yes "$PVE" "cp $DST/iptv-remux.service /etc/systemd/system/ && systemctl daemon-reload && systemctl enable iptv-remux"
fi

ssh -o BatchMode=yes "$PVE" 'systemctl restart iptv-remux && sleep 2 && systemctl is-active iptv-remux && curl -fsS http://127.0.0.1:8877/buffer'
echo "deployed OK"
