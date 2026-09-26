#!/bin/sh
set -eu
psql -v ON_ERROR_STOP=1 \
  -v owner_password="$ORBIT_OWNER_PASSWORD" \
  -v app_password="$ORBIT_APP_PASSWORD" \
  --username "$POSTGRES_USER" --dbname postgres \
  -f /bootstrap/bootstrap-roles.sql
