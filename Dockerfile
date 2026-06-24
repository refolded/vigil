FROM python:3.11-slim

# Install Docker CLI (for docker ps / start / stop via mounted socket)
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl gnupg lsb-release && \
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/debian/gpg \
      | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
      https://download.docker.com/linux/debian $(lsb_release -cs) stable" \
      > /etc/apt/sources.list.d/docker.list && \
    apt-get update && apt-get install -y --no-install-recommends docker-ce-cli && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/  ./app/
COPY static/ ./static/

# Cache vendor assets at build time for offline / air-gapped use
RUN mkdir -p /app/static/vendor && \
    curl -sL "https://cdn.tailwindcss.com"                                         -o /app/static/vendor/tailwind.js && \
    curl -sL "https://cdn.jsdelivr.net/npm/alpinejs@3/dist/cdn.min.js"             -o /app/static/vendor/alpine.min.js

EXPOSE 8474

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8575/api/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8474", "--log-level", "warning"]
