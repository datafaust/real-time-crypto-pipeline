#!/usr/bin/env bash
set -euo pipefail

echo "[pred-stats-refresher] booting..."

# default interval if not provided via env
: "${PRED_STATS_REFRESH_MINUTES:=15}"

# wait for Postgres to accept TCP
until pg_isready -h "$PGHOST" -p "$PGPORT" -d "$PGDATABASE" -U "$PGUSER" >/dev/null 2>&1; do
  echo "[pred-stats-refresher] waiting for postgres..."
  sleep 2
done

printf "[pred-stats-refresher] starting loop (every %sm)\n" "$PRED_STATS_REFRESH_MINUTES"

while true; do
  echo "[pred-stats-refresher] $(date -Iseconds) refresh CONCURRENTLY..."
  PGOPTIONS="--statement-timeout=300000" \
  psql -v ON_ERROR_STOP=1 \
       -c "REFRESH MATERIALIZED VIEW CONCURRENTLY public.pred_stats_30d_mv;"
  echo "[pred-stats-refresher] done. sleeping..."
  sleep "${PRED_STATS_REFRESH_MINUTES}m"
done
