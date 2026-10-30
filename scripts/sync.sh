#!/usr/bin/env bash
# Pull finished days from the VPS to the laptop, then delete them on the VPS — but only
# after a checksum pass confirms the copy is identical, and only files older than KEEP_DAYS.
# Run on the laptop (the VPS cannot reach it):
#
#   scripts/sync.sh root@vps:/root/l2-tape/data
#
# Env: DST (local data dir, default ./data), KEEP_DAYS (default 3), SSH (default ssh).
set -euo pipefail

src=$1
host=${src%%:*}
rdir=${src#*:}
dst=${DST:-data}
keep=${KEEP_DAYS:-3}
ssh=${SSH:-ssh}
today=$(date -u +%Y%m%d)

# Today's raw files are still being written; temp files are never final.
skip=(--exclude "*_${today}.gz" --exclude '*.tmp')

rsync -a -e "$ssh" "${skip[@]}" "$src/" "$dst/"

# Verify: a checksum dry run must find nothing left to transfer.
diff=$(rsync -anc -e "$ssh" "${skip[@]}" --out-format='%n' "$src/" "$dst/" | grep -v '/$' || true)
if [[ -n $diff ]]; then
  echo "copy differs from VPS, nothing deleted:" >&2
  echo "$diff" >&2
  exit 1
fi

$ssh "$host" "find '$rdir/raw' '$rdir/snapshots' -type f ! -name '*_${today}.gz' ! -name '*.tmp' \
  -mtime +$keep -print -delete && find '$rdir/snapshots' -mindepth 1 -type d -empty -delete"
echo "synced $src -> $dst; VPS files older than $keep days removed (listed above)"
