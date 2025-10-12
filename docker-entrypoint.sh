#!/bin/sh
set -e

CONFIG_DIR="${DASHFOLIO_CONFIG_DIR:-/config}"
mkdir -p "$CONFIG_DIR"

if [ "${DASHFOLIO_SKIP_BOOTSTRAP:-0}" != "1" ]; then
    echo "[dashfolio] Running initial risk analysis bootstrap..."
    if ! python main.py; then
        echo "[dashfolio] Warning: initial bootstrap failed; continuing startup." >&2
    fi
fi

exec "$@"
