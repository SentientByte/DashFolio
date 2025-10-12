# Stage 1: run front-end tests to keep the bundle healthy
FROM node:20-alpine AS frontend-tests
WORKDIR /app
COPY package.json package-lock.json tsconfig.json vitest.config.ts ./
COPY portfolio ./portfolio
RUN npm ci
RUN npm test

# Stage 2: build the Python runtime image
FROM python:3.11-slim AS runtime
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DASHFOLIO_CONFIG_DIR=/config
WORKDIR /app

COPY requirements.txt ./
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r requirements.txt

COPY . .
RUN useradd --create-home dashfolio \
    && chown -R dashfolio:dashfolio /app
RUN chmod +x docker-entrypoint.sh

USER dashfolio
VOLUME ["/config"]
EXPOSE 5000

ENTRYPOINT ["./docker-entrypoint.sh"]
CMD ["gunicorn", "-b", "0.0.0.0:5000", "app:app"]
