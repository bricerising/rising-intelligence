WITH duplicate_snapshots AS (
  SELECT
    id,
    ROW_NUMBER() OVER (
      PARTITION BY "window", "generated_at"
      ORDER BY id ASC
    ) AS row_number
  FROM "brief_trend_snapshots"
)
DELETE FROM "brief_trend_snapshots"
WHERE id IN (
  SELECT id
  FROM duplicate_snapshots
  WHERE row_number > 1
);

CREATE UNIQUE INDEX "uq_brief_trend_snapshots_window_generated"
ON "brief_trend_snapshots"("window", "generated_at");
