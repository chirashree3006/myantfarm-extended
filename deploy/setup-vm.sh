#!/usr/bin/env bash
# =====================================================================
#  One-shot cloud VM setup for MyAntFarm-Extended (Ubuntu 22.04 / 24.04,
#  x86_64 or ARM/Ampere). Works on Oracle Cloud, Azure, AWS, GCP, DO.
#
#  Usage on the VM:
#     curl -fsSL https://raw.githubusercontent.com/<you>/myantfarm-extended/main/deploy/setup-vm.sh \
#        | sudo bash -s -- https://github.com/<you>/myantfarm-extended.git
#  or, after cloning:
#     sudo bash deploy/setup-vm.sh
#
#  Also usable as an Oracle/Azure "cloud-init" user-data script: set
#  REPO_URL below and paste the whole file into the cloud-init box.
# =====================================================================
set -euo pipefail

REPO_URL="${1:-${REPO_URL:-}}"
APP_DIR="${APP_DIR:-/opt/myantfarm-extended}"
APP_USER="${SUDO_USER:-ubuntu}"

log() { echo -e "\n\033[1;32m[setup]\033[0m $*"; }

[ "$(id -u)" -eq 0 ] || { echo "run with sudo"; exit 1; }

log "1/6 system packages"
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y ca-certificates curl git iptables-persistent netfilter-persistent || \
  apt-get install -y ca-certificates curl git

log "2/6 Docker Engine + Compose plugin"
if ! command -v docker >/dev/null; then
  curl -fsSL https://get.docker.com | sh
fi
systemctl enable --now docker
id "$APP_USER" >/dev/null 2>&1 && usermod -aG docker "$APP_USER" || true

log "3/6 host firewall: open 80, 443 and 8080"
# Oracle's Ubuntu images ship an iptables REJECT rule that blocks everything
# except 22 even when the cloud security list allows it -- insert ACCEPTs
# above it. Harmless on other clouds.
for p in 80 443 8080; do
  iptables -C INPUT -p tcp --dport "$p" -m state --state NEW -j ACCEPT 2>/dev/null || \
    iptables -I INPUT 5 -p tcp --dport "$p" -m state --state NEW -j ACCEPT
done
command -v netfilter-persistent >/dev/null && netfilter-persistent save || true
command -v ufw >/dev/null && ufw status | grep -q active && { ufw allow 80/tcp; ufw allow 443/tcp; ufw allow 8080/tcp; } || true

log "4/6 swap (helps small VMs run TinyLlama)"
if ! swapon --show | grep -q /swapfile; then
  MEM_GB=$(awk '/MemTotal/ {printf "%d", $2/1024/1024}' /proc/meminfo)
  if [ "$MEM_GB" -lt 8 ]; then
    fallocate -l 4G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
    echo '/swapfile none swap sw 0 0' >> /etc/fstab
  fi
fi

log "5/6 project code"
if [ -f "./docker-compose.yml" ] && [ -z "$REPO_URL" ]; then
  APP_DIR="$(pwd)"
elif [ -d "$APP_DIR/.git" ]; then
  git -C "$APP_DIR" pull --ff-only
else
  [ -n "$REPO_URL" ] || { echo "Pass the git repo URL as the first argument"; exit 1; }
  git clone "$REPO_URL" "$APP_DIR"
fi
chown -R "$APP_USER":"$APP_USER" "$APP_DIR" 2>/dev/null || true
cd "$APP_DIR"
[ -f .env ] || cp .env.example .env
# a real session-signing secret
if grep -q '^JWT_SECRET=change-me-please' .env; then
  sed -i "s/^JWT_SECRET=.*/JWT_SECRET=$(openssl rand -hex 32)/" .env
fi
# optional: DOMAIN and GOOGLE_CLIENT_ID can be passed as env vars to this script
[ -n "${DOMAIN:-}" ] && sed -i "s/^DOMAIN=.*/DOMAIN=${DOMAIN}/" .env
[ -n "${GOOGLE_CLIENT_ID:-}" ] && sed -i "s/^GOOGLE_CLIENT_ID=.*/GOOGLE_CLIENT_ID=${GOOGLE_CLIENT_ID}/" .env
DOMAIN_SET=$(grep -E '^DOMAIN=.+' .env | cut -d= -f2 || true)

# Small VM (<6 GB RAM)? run a single TinyLlama replica.
MEM_GB=$(awk '/MemTotal/ {printf "%d", $2/1024/1024}' /proc/meminfo)
COMPOSE="docker compose"
if [ "$MEM_GB" -lt 6 ]; then
  log "low memory (${MEM_GB} GB) -> lite mode: 1 TinyLlama replica"
  COMPOSE="docker compose -f docker-compose.yml -f docker-compose.lite.yml"
fi
if [ -n "$DOMAIN_SET" ]; then
  log "HTTPS for $DOMAIN_SET via Caddy (Let's Encrypt)"
  sed -i "s/^GATEWAY_PORT=.*/GATEWAY_PORT=8000/" .env
  COMPOSE="$COMPOSE --profile tls"
fi

log "6/6 build + start (first run pulls ~2 GB of images + the 640 MB TinyLlama model)"
$COMPOSE up -d --build

log "waiting for the model to be ready (can take a few minutes on first boot)..."
GW=$( [ -n "$DOMAIN_SET" ] && echo "http://localhost:8000" || echo "http://localhost" )
for i in $(seq 1 120); do
  if curl -fs $GW/api/multi/metrics | grep -q '"model_loaded":true'; then break; fi
  sleep 5
done

IP=$(curl -fs https://api.ipify.org || hostname -I | awk '{print $1}')
HOST=$( [ -n "$DOMAIN_SET" ] && echo "https://$DOMAIN_SET" || echo "http://$IP" )
log "DONE"
echo "  Shop        : $HOST/"
echo "  Ops console : $HOST/ops/"
echo "  Website LB  : http://$IP:8080/"
echo "  Status    : cd $APP_DIR && $COMPOSE ps"
