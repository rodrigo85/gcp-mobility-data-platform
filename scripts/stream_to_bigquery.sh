#!/usr/bin/env bash
# Bounded end-to-end streaming run against real Pub/Sub and BigQuery.
#
# Starts the Beam pipeline (DirectRunner, BigQuery sink), publishes simulated
# rides for SIM_SECONDS, lets Beam flush its last load jobs, then prints row
# counts of the three bronze streaming tables.
#
# The DirectRunner sustains ~3 events/s on the VPS (one Python process), so the
# default rate stays below that. A faster publisher only builds a backlog that
# the next run would inherit; this is the main reason production runs on Dataflow.
#
# Runs on the VPS host (see `make vps-stream`). Requires:
#   - GCP_PROJECT_ID
#   - GOOGLE_APPLICATION_CREDENTIALS: key of the mobility-pipelines service account
#   - the Beam venv from `make vps-setup`
# Cost: Pub/Sub and BigQuery load jobs are free at this volume; a few MB scanned.
set -euo pipefail

: "${GCP_PROJECT_ID:?set GCP_PROJECT_ID}"
: "${GOOGLE_APPLICATION_CREDENTIALS:?set GOOGLE_APPLICATION_CREDENTIALS}"
unset PUBSUB_EMULATOR_HOST

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PY="${PY:-$REPO_DIR/.venvs/beam/bin/python}"
SIM_SECONDS="${SIM_SECONDS:-180}"
SIM_RATE="${SIM_RATE:-0.5}"
TRIGGER_SECONDS="${TRIGGER_SECONDS:-60}"
WORK_DIR="${WORK_DIR:-/tmp/mobility-stream}"
P="$GCP_PROJECT_ID"

rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"
cd "$REPO_DIR"

# Start clean: drop any backlog left on the subscription by an earlier run
gcloud pubsub subscriptions seek ride-events-beam --project "$P" \
  --time "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > /dev/null
START_TS="$(date -u '+%Y-%m-%d %H:%M:%S')"

# Beam warm-up + simulation + two trigger periods to flush the last panes
BEAM_SECONDS=$((40 + SIM_SECONDS + 2 * TRIGGER_SECONDS + 60))
echo "beam: up to ${BEAM_SECONDS}s, log at $WORK_DIR/beam.log"
timeout "$BEAM_SECONDS" "$PY" -m mobility_beam.streaming_rides \
  --runner DirectRunner --streaming \
  --project "$P" --temp_location "gs://$P-mobility-lake/dataflow/tmp" \
  --input_subscription "projects/$P/subscriptions/ride-events-beam" \
  --sink bigquery --bq_project "$P" --bq_triggering_frequency "$TRIGGER_SECONDS" \
  > "$WORK_DIR/beam.log" 2>&1 &
BEAM_PID=$!

sleep 40
echo "simulator: ${SIM_RATE} rides/s (~4 events per ride) for ${SIM_SECONDS}s"
PYTHONPATH=simulator "$PY" -m ride_simulator --project "$P" --topic ride-events \
  --rate "$SIM_RATE" --duration-seconds "$SIM_SECONDS" \
  --late-fraction 0.05 --duplicate-fraction 0.05 --malformed-fraction 0.02 2>&1 | tail -n 1

# timeout ends the unbounded pipeline with exit code 124: that is the expected path
wait "$BEAM_PID" || true

echo "beam errors (empty is good):"
grep -E "Traceback|ERROR:" "$WORK_DIR/beam.log" | head -5 || true

echo "bronze rows loaded by this run:"
bq query --project_id="$P" --use_legacy_sql=false --format=pretty \
  --maximum_bytes_billed=500000000 "
select 'ride_events' as table_name, count(*) as new_rows, max(ingest_ts) as last_loaded,
  format('%d rows with event_ts before the run (backlog)', countif(event_ts < timestamp '$START_TS')) as check_
from \`$P.bronze.ride_events\` where ingest_ts >= timestamp '$START_TS'
union all
select 'zone_demand_1m', count(*), max(ingest_ts), format('%d LATE panes', countif(pane_timing = 'LATE'))
from \`$P.bronze.zone_demand_1m\` where ingest_ts >= timestamp '$START_TS'
union all
select 'ride_events_dlq', count(*), max(received_ts), string_agg(distinct error, ', ' limit 3)
from \`$P.bronze.ride_events_dlq\` where received_ts >= timestamp '$START_TS'"
