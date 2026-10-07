# One image for every Python microservice (agents, coordinator, single-agent).
# docker-compose picks which one runs via the `command:` of each service.
FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY antfarm ./antfarm
COPY services ./services
COPY multi_agent.py .

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

CMD ["uvicorn", "services.coordinator_service:app", "--host", "0.0.0.0", "--port", "8000"]
