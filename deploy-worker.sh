#!/usr/bin/env bash
set -euo pipefail
HOST=ubuntu@hazshield-compute
DEST=/opt/hazshield-workers
SRC=$(pwd)          # repo subdir with worker.py, requirements.txt, unit file

SHA=$(git rev-parse --short HEAD)
[ -n "$(git status --porcelain)" ] && SHA="${SHA}-dirty" && echo "WARN: deploying dirty tree"

# ship code + a version marker
scp -q "$SRC/worker.py" "$SRC/requirements.txt" "$SRC/episodes.py" "$HOST:$DEST/"
echo "$SHA" | ssh "$HOST" "cat > $DEST/VERSION"

ssh "$HOST" "
  $DEST/venv/bin/pip install -q -r $DEST/requirements.txt &&
  sudo systemctl restart hazshield-worker@1 &&
  sleep 2 &&
  sudo systemctl is-active --quiet hazshield-worker@1
" || { echo '✗ service failed to start:'; ssh "$HOST" 'journalctl -u hazshield-worker@1 -n 8 --no-pager'; exit 1; }

# verification: the running file must be byte-identical to what we built
LOCAL=$(sha256sum "$SRC/worker.py" | cut -d' ' -f1)
REMOTE=$(ssh "$HOST" "sha256sum $DEST/worker.py | cut -d' ' -f1")
if [ "$LOCAL" = "$REMOTE" ]; then
    echo "✓ deploy verified: $SHA live on compute ($(echo $REMOTE | head -c 12)…)"
else
    echo "✗ CHECKSUM MISMATCH — remote file differs from repo" >&2; exit 1
fi
