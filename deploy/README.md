# Deploy Optimus Gateway

Pipeline: `Jenkinsfile` -> build -> scp ke `/home/halim/optimus/gateway.new` -> `remote-deploy.sh` (ganti binary, restart systemd, rollback jika gagal).

## Setup satu kali di server tujuan
1. Public key user Jenkins (di server Jenkins) dipasang di `/home/halim/.ssh/authorized_keys`.
2. User `halim` harus bisa restart service tanpa password, buat `/etc/sudoers.d/optimus-deploy`:
   ```
   halim ALL=(root) NOPASSWD: /usr/bin/systemctl restart optimus-gateway, /usr/bin/systemctl is-active --quiet optimus-gateway, /usr/bin/journalctl -u optimus-gateway -n 30 --no-pager
   ```
   (cek path `systemctl` dengan `which systemctl`)
3. Folder `/home/halim/optimus` harus bisa ditulis user `halim`:
   `sudo chown -R halim:halim /home/halim/optimus`
4. Unit systemd: lihat `optimus-gateway.service`.

## Setup Jenkins
- Job tipe Multibranch Pipeline (deploy hanya jalan di branch `main`).
- Isi `DEPLOY_HOST` di `Jenkinsfile`.
- Variabel global `GCHAT_WEBHOOK` dan plugin Google Chat Notification harus tersedia.
- Go 1.23+ ada di agent Jenkins.

## Rollback
Setiap deploy menyimpan binary versi sebelumnya sebagai `gateway.<commit-id>` (default 5 terakhir, `KEEP_RELEASES`).
Commit yang sedang jalan dicatat di `.current_commit`, sebelumnya di `.previous_commit`.

```bash
# di server, sebagai user halim
bash /home/halim/optimus/rollback.sh              # ke commit sebelumnya
bash /home/halim/optimus/rollback.sh <commit-id>  # ke commit tertentu

# atau dari mesin Jenkins
ssh halim@<host> 'bash -s' < deploy/rollback.sh
ssh halim@<host> 'bash -s -- <commit-id>' < deploy/rollback.sh
```

Lihat versi tersedia: `ls -t /home/halim/optimus/gateway.*` dan `cat /home/halim/optimus/.current_commit`.
Kalau service gagal start setelah rollback, binary dikembalikan seperti semula.
Menjalankan rollback tanpa argumen lagi = kembali ke versi yang tadi.
