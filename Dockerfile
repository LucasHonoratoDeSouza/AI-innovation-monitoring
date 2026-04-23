FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY config ./config

RUN apt-get update \
    && apt-get install -y --no-install-recommends firefox-esr \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --no-cache-dir .

EXPOSE 8080

CMD ["ai-monitor", "serve", "--host", "0.0.0.0", "--port", "8080"]
