#!/bin/bash
set -e

CRDB_HOST="cockroachdb"
CRDB_PORT="26257"

echo "Waiting for CockroachDB to be ready..."
until /cockroach/cockroach sql --insecure --host="$CRDB_HOST" --port="$CRDB_PORT" -e "SELECT 1" > /dev/null 2>&1; do
  sleep 1
done
echo "CockroachDB is ready."

/cockroach/cockroach sql --insecure --host="$CRDB_HOST" --port="$CRDB_PORT" <<'SQL'

CREATE DATABASE IF NOT EXISTS loadtest;

USE loadtest;

CREATE TABLE IF NOT EXISTS events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_type STRING NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id STRING NOT NULL,
  amount DECIMAL NOT NULL,
  status STRING NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

SET CLUSTER SETTING kv.rangefeed.enabled = true;

CREATE CHANGEFEED FOR events, orders
  INTO 'kafka://kafka:9093'
  WITH format = json,
       updated,
       resolved = '5s',
       diff,
       kafka_sink_config = '{"Flush":{"MaxMessages":100,"Frequency":"500ms"}}';

SQL

echo "Database, tables, and changefeed created successfully."
