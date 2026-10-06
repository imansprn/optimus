#!/usr/bin/env bash
# Rollback binary gateway. Jalankan di server sebagai user halim:
#   bash /home/halim/optimus/rollback.sh              # ke commit sebelumnya
#   bash /home/halim/optimus/rollback.sh <commit-id>  # ke commit tertentu
# atau dari mesin Jenkins:
#   ssh halim@<host> 'bash -s' < deploy/rollback.sh
#   ssh halim@<host> 'bash -s -- <commit-id>' < deploy/rollback.sh
#
# Versi yang sedang jalan tetap disimpan sebagai gateway.<commit-id>,
# jadi menjalankan script ini lagi tanpa argumen = kembali ke versi tadi.
set -euo pipefail

APP_DIR="${APP_DIR:-/home/halim/optimus}"
SERVICE="${SERVICE:-optimus-gateway}"
cd "$APP_DIR"

CUR="$(cat .current_commit 2>/dev/null || echo unknown)"
TARGET="${1:-$(cat .previous_commit 2>/dev/null || true)}"

[ -n "$TARGET" ] || { echo "Tidak ada commit sebelumnya (.previous_commit kosong). Sebutkan commit-id."; exit 1; }
[[ "$TARGET" =~ ^[A-Za-z0-9]+$ ]] || { echo "commit-id tidak valid: $TARGET"; exit 1; }
[ "$TARGET" != "$CUR" ] || { echo "Commit $TARGET sudah yang sedang berjalan"; exit 1; }
[ -f "gateway.$TARGET" ] || { echo "gateway.$TARGET tidak ada. Tersedia:"; ls -t gateway.* 2>/dev/null | grep -E '^gateway\.[A-Za-z0-9]+$' || true; exit 1; }

restart_and_check() {
  sudo -n systemctl restart "$SERVICE"
  sleep 5
  sudo -n systemctl is-active --quiet "$SERVICE"
}

use_binary() {  # $1 = file sumber
  cp "$1" gateway.tmp
  chmod +x gateway.tmp
  mv -f gateway.tmp gateway
}

echo "Rollback: $CUR -> $TARGET"
# Pastikan versi sekarang tersimpan agar bisa dikembalikan
[ -f "gateway.$CUR" ] || cp gateway "gateway.$CUR"
use_binary "gateway.$TARGET"

if restart_and_check; then
  echo "$CUR" > .previous_commit
  echo "$TARGET" > .current_commit
  echo "Rollback OK: $SERVICE aktif (commit $TARGET)"
  exit 0
fi

echo "Service gagal start setelah rollback, mengembalikan ke $CUR..."
sudo -n journalctl -u "$SERVICE" -n 30 --no-pager || true
use_binary "gateway.$CUR"
restart_and_check || echo "PERINGATAN: $SERVICE masih tidak aktif, cek manual"
exit 1
