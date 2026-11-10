#!/usr/bin/env python3
"""Rewrite provider m3u_plus playlist: live channel URLs -> local remux proxy.
VOD/series entries (containing /movie/ or /series/) are left untouched."""
import os
import re

SRC = "/root/iptv-remux/provider.m3u"
DST = "/root/iptv-remux/full.m3u"
PROXY = "http://192.168.1.42:8877/ch/{}/index.m3u8"
pat = re.compile(r"^https?://[^/]+/\S+/\S+/(\d+)$")

out, n = [], 0
for l in open(SRC, errors="replace"):
    l = l.rstrip("\n")
    m = pat.match(l.strip())
    if m and "/movie/" not in l and "/series/" not in l:
        out.append(PROXY.format(m.group(1)))
        n += 1
    else:
        out.append(l)

# Atomic publish: a reader mid-write must never see a partial playlist.
tmp = DST + ".tmp"
with open(tmp, "w") as fh:
    fh.write("\n".join(out) + "\n")
os.replace(tmp, DST)
print(f"rewrote {n} live entries -> {DST}")
