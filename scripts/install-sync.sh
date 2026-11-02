#!/usr/bin/env bash
# Run scripts/sync.sh every day at 09:00 local time via launchd; if the laptop was asleep
# then, launchd runs it on wake. A failure shows a macOS notification; the log is
# data/logs/sync.log. ssh runs in BatchMode: a missing key fails at once instead of hanging.
#
#   scripts/install-sync.sh root@vps:/root/l2-tape/data   # install or update
#   launchctl kickstart gui/$(id -u)/com.l2tape.sync      # run now
#   launchctl bootout gui/$(id -u)/com.l2tape.sync        # remove
set -euo pipefail

src=$1
repo=$(cd "$(dirname "$0")/.." && pwd)
label=com.l2tape.sync
plist=~/Library/LaunchAgents/$label.plist

cmd="cd '$repo' && mkdir -p data/logs && { date -u; SSH='ssh -o BatchMode=yes' scripts/sync.sh $src; } >> data/logs/sync.log 2>&1 || osascript -e 'display notification \"data/logs/sync.log\" with title \"l2-tape: sync failed\"'"

mkdir -p ~/Library/LaunchAgents
cat > "$plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$label</string>
  <key>ProgramArguments</key>
  <array><string>/bin/bash</string><string>-c</string><string>${cmd//&/&amp;}</string></array>
  <key>StartCalendarInterval</key>
  <dict><key>Hour</key><integer>9</integer><key>Minute</key><integer>0</integer></dict>
</dict>
</plist>
EOF
plutil -lint "$plist"
launchctl bootout "gui/$(id -u)/$label" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$plist"
echo "installed $plist: daily 09:00, log $repo/data/logs/sync.log"
