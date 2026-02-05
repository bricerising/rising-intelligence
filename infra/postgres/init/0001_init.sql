-- rising-intelligence Postgres read model (MVP)
-- This schema is intentionally small and append-only.

CREATE TABLE IF NOT EXISTS trend_snapshots (
  id BIGSERIAL PRIMARY KEY,
  generated_at TIMESTAMPTZ NOT NULL,
  window TEXT NOT NULL,
  snapshot JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trend_snapshots_window_generated_at
  ON trend_snapshots (window, generated_at DESC);

CREATE TABLE IF NOT EXISTS brief_results (
  request_id TEXT PRIMARY KEY,
  produced_at TIMESTAMPTZ NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('success', 'failure')),
  result JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_brief_results_produced_at
  ON brief_results (produced_at DESC);

-- Convenience views for Grafana dashboards (best-effort; depends on JSON shape).
CREATE OR REPLACE VIEW trend_topic_metrics AS
SELECT
  ts.id AS snapshot_id,
  ts.generated_at,
  ts.window,
  (topic->>'topic') AS topic,
  NULLIF(topic->>'window_end', '')::timestamptz AS window_end,
  NULLIF(topic->>'volume', '')::int AS volume,
  NULLIF(topic->>'prev_volume', '')::int AS prev_volume,
  NULLIF(topic->>'acceleration', '')::double precision AS acceleration,
  NULLIF(topic->>'baseline_volume', '')::int AS baseline_volume,
  NULLIF(topic->>'baseline_delta', '')::double precision AS baseline_delta,
  NULLIF(topic->>'score', '')::double precision AS score,
  topic->'evidence' AS evidence
FROM trend_snapshots ts,
LATERAL jsonb_array_elements(ts.snapshot->'topics') AS topic;

CREATE OR REPLACE VIEW brief_highlights AS
SELECT
  br.request_id,
  br.produced_at,
  (h->>'topic') AS topic,
  h->>'why_it_matters' AS why_it_matters,
  h->>'what_happened' AS what_happened,
  h->>'suggested_action' AS suggested_action,
  h->'citations' AS citations
FROM brief_results br,
LATERAL jsonb_array_elements(br.result->'brief'->'highlights') AS h
WHERE br.status = 'success';

