#!/usr/bin/env bash
# Runs every time the codespace starts: build + start the stack, wait for TinyLlama.
set -e
cd "$(dirname "$0")/.."
[ -f .env ] || cp .env.example .env
grep -q '^JWT_SECRET=change-me-please' .env && sed -i "s/^JWT_SECRET=.*/JWT_SECRET=$(openssl rand -hex 32)/" .env
docker compose up -d --build
echo "Waiting for TinyLlama (first start downloads ~640 MB)..."
for i in $(seq 1 120); do
  curl -fs http://localhost/api/multi/metrics | grep -q '"model_loaded":true' && break
  sleep 5
done
if [ -n "$CODESPACE_NAME" ]; then
  URL="https://${CODESPACE_NAME}-80.${GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN}"
  gh codespace ports visibility 80:public -c "$CODESPACE_NAME" >/dev/null 2>&1 || true
  echo "Shop        : $URL/"
  echo "Ops console : $URL/ops/"
fi
