-- Add consumer_lag retention policy (missing from initial seed)
INSERT INTO "retention_policies" ("table_name", "retention_days", "enabled")
VALUES ('consumer_lag', 7, true)
ON CONFLICT ("table_name") DO NOTHING;

