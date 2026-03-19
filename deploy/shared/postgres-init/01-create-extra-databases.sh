#!/bin/sh
set -eu

create_db() {
  db_name="$1"
  if [ -z "${db_name}" ]; then
    return 0
  fi

  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<EOF
SELECT format('CREATE DATABASE %I', '${db_name}')
WHERE NOT EXISTS (
  SELECT FROM pg_database WHERE datname = '${db_name}'
)\gexec
EOF
}

create_db "${RIVEN_POSTGRES_DB:-}"
create_db "${ZILEAN_POSTGRES_DB:-}"
