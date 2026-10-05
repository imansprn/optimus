#!/usr/bin/env bash
# Dijalankan di server sebagai user halim (dipanggil Jenkins via SSH).
# Restart service memakai sudo (butuh sudoers NOPASSWD, lihat deploy/README.md).
# Ganti binary dengan gateway.new, restart service, rollback otomatis kalau gagal.
set -euo pipefail

APP_DIR="${APP_DIR:-/home/halim/optimus}"
SERVICE="${SERVICE:-optimus-gateway}"
cd "$APP_DIR"

[ -f gateway.new ] || { echo "gateway.new tidak ditemukan"; exit 1; }
chmod +x gateway.new

[ -f gateway ] && cp -p gateway gateway.prev
mv -f gateway.new gateway

sudo -n systemctl restart "$SERVICE"
sleep 5

if sudo -n systemctl is-active --quiet "$SERVICE"; then
  echo "Deploy OK: $SERVICE aktif"
  exit 0
fi

echo "Service gagal start, rollback..."
sudo -n journalctl -u "$SERVICE" -n 30 --no-pager || true
if [ -f gateway.prev ]; then
  mv -f gateway.prev gateway
  sudo -n systemctl restart "$SERVICE"
fi
exit 1
