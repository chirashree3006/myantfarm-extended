#!/bin/sh
# Start the Ollama server, pull the model once (shared volume), keep serving.
set -e
MODEL="${MODEL:-tinyllama}"

ollama serve &
PID=$!

echo "[ollama] waiting for server..."
until ollama list >/dev/null 2>&1; do sleep 1; done

if ollama list | grep -q "^${MODEL}"; then
  echo "[ollama] ${MODEL} already present"
else
  echo "[ollama] pulling ${MODEL} (first start only, ~640MB)..."
  ollama pull "${MODEL}"
fi

# Warm the model into RAM so the first agent call isn't slow.
ollama run "${MODEL}" "hi" >/dev/null 2>&1 || true
echo "[ollama] ready"
touch /tmp/ready

wait $PID
