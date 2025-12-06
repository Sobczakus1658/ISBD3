#!/bin/sh
set -e

if [ -d /data ] && [ "$(ls -A /data)" ]; then
    mkdir -p /app/data
    cp -a /data/* /app/data/ 2>/dev/null || true
fi

exec "$@"
