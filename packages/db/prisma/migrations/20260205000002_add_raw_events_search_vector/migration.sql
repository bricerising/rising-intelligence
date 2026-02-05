-- Add full-text search support for raw_events.
-- Prisma does not support generated columns, so this is maintained via raw SQL.

ALTER TABLE "raw_events"
ADD COLUMN IF NOT EXISTS "search_vector" tsvector
GENERATED ALWAYS AS (
  setweight(to_tsvector('english', coalesce("title", '')), 'A') ||
  setweight(to_tsvector('english', coalesce("text", '')), 'B')
) STORED;

CREATE INDEX IF NOT EXISTS "idx_raw_events_search"
ON "raw_events" USING GIN ("search_vector");

