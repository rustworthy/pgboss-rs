#! /usr/bin/env bash

DATABASE_HOST=${1:-127.0.0.1}
DATABASE_PORT=${2:-5432}

if ! [ -x "$(command -v docker)" ]; then
    echo '❌ Error: please make sure docker is installed and is in the PATH' >&2
    exit 1
fi
if ! (docker compose version >/dev/null) then
    echo '❌ Error: please make sure compose plugin for docker is installed' >&2
    exit 1
fi

echo "🐘 Spinning up an instance of PostgreSQL"
(DATABASE_HOST=$DATABASE_HOST DATABASE_PORT=$DATABASE_PORT docker compose \
    -f docker/compose.yaml up -d --build)

while ! (nc -z ${DATABASE_HOST} ${DATABASE_PORT} >/dev/null); do
    echo "⏳ Waiting for the database to start accepting connections..."
    sleep 2
done

echo "✅ PostgreSQL server is ready at $DATABASE_HOST:$DATABASE_PORT"