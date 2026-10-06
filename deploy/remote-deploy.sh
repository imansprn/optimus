#!/usr/bin/env bash
# Dijalankan di server sebagai user halim (dipanggil Jenkins via SSH).
# Restart service memakai sudo (butuh sudoers NOPASSWD, lihat deploy/README.md).
#
# Ganti binary dengan gateway.new, restart service, rollback otomatis kalau gagal.
# Binary versi sebelumnya disimpan sebagai gateway.<commit-id> (maks KEEP_RELEASES).
# Env wajib: COMMIT_ID (git short hash yang sedang di-deploy)
set -euo pipefail

APP_DIR="${APP_DIR:-/home/halim/optimus}"
SERVICE="${SERVICE:-optimus-gateway}"
KEEP_RELEASES="${KEEP_RELEASES:-5}"
NEW="${COMMIT_ID:?COMMIT_ID wajib diisi}"
[[ "$NEW" =~ ^[A-Za-z0-9]+$ ]] || { echo "COMMIT_ID tidak valid: $NEW"; exit 1; }

cd "$APP_DIR"
[ -f gateway.new ] || { echo "gateway.new tidak ditemukan"; exit 1; }
chmod +x gateway.new

CUR="$(cat .current_commit 2>/dev/null || echo unknown)"

# Simpan binary yang sedang jalan sebagai gateway.<commit-lama>
if [ -f gateway ] && [ "$CUR" != "$NEW" ]; then
  cp gateway "gateway.$CUR"
fi
mv -f gateway.new gateway

sudo -n systemctl restart "$SERVICE"
sleep 5

if sudo -n systemctl is-active --quiet "$SERVICE"; then
  if [ "$CUR" != "$NEW" ]; then
    echo "$CUR" > .previous_commit
  fi
  echo "$NEW" > .current_commit
  echo "Deploy OK: $SERVICE aktif (commit $NEW, sebelumnya $CUR)"

  # Hapus release lama: sisakan KEEP_RELEASES terbaru selain current & previous (yang selalu dijaga)
  PREV="$(cat .previous_commit 2>/dev/null || true)"
  ls -t gateway.* 2>/dev/null \
    | grep -E '^gateway\.[A-Za-z0-9]+$' \
    | grep -vE "^gateway\.(new|$NEW|$PREV)$" \
    | tail -n +"$((KEEP_RELEASES + 1))" \
    | xargs -r rm -f --
  exit 0
fi

echo "Service gagal start, rollback ke $CUR..."
sudo -n journalctl -u "$SERVICE" -n 30 --no-pager || true
if [ -f "gateway.$CUR" ]; then
  cp "gateway.$CUR" gateway.restore
  chmod +x gateway.restore
  mv -f gateway.restore gateway
  sudo -n systemctl restart "$SERVICE"
fi
exit 1
