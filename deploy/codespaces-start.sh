#!/usr/bin/env bash
# Runs every time the codespace starts: build + start the stack, wait for TinyLlama.
# Codespaces keeps Docker on a small system disk; the Ollama image alone is several GB.
# So Docker's storage is moved to /tmp, which sits on the large disk.
set -e
cd "$(dirname "$0")/.."

ROOT_DIR=$(docker info --format '{{.DockerRootDir}}' 2>/dev/null || echo "")
if [ "$ROOT_DIR" != "/tmp/docker" ]; then
  echo "Moving Docker storage to /tmp (big disk)..."
  sudo pkill dockerd 2>/dev/null || true
  sleep 3
  sudo mkdir -p /tmp/docker
  sudo bash -c 'nohup dockerd --data-root /tmp/docker > /tmp/dockerd.log 2>&1 &'
  for i in $(seq 1 30); do docker info >/dev/null 2>&1 && break; sleep 1; done
fi
echo "Docker root: $(docker info --format '{{.DockerRootDir}}')"
df -h / /tmp | sed 's/^/  /'

[ -f .env ] || cp .env.example .env
if grep -q '^JWT_SECRET=change-me-please' .env; then
  sed -i "s/^JWT_SECRET=.*/JWT_SECRET=$(openssl rand -hex 32)/" .env
fi

docker compose up -d --build
docker builder prune -af >/dev/null 2>&1 || true   # drop build cache, keep images

echo "Waiting for TinyLlama (first start downloads ~640 MB)..."
for i in $(seq 1 120); do
  curl -fs http://localhost/api/multi/metrics | grep -q '"model_loaded":true' && break
  sleep 5
done
docker compose ps --format 'table {{.Service}}\t{{.Status}}'

if [ -n "$CODESPACE_NAME" ]; then
  URL="https://${CODESPACE_NAME}-80.${GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN}"
  gh codespace ports visibility 80:public -c "$CODESPACE_NAME" >/dev/null 2>&1 || true
  echo "Shop        : $URL/"
  echo "Ops console : $URL/ops/"
  echo "If the link doesn't open: Ports tab -> Forward a Port -> 80 -> right-click -> Port visibility -> Public"
fi
