# Spec 007: Operations Runbook

**Created**: 2026-02-05
**Status**: Proposed

## Overview

This runbook documents common operational procedures for the Rising Intelligence system. Use this when things go wrong or when performing routine maintenance.

## Quick Reference

### Service URLs (Local Dev)

| Service | URL |
|---------|-----|
| Grafana | http://localhost:3001 (admin/admin) |
| Redpanda Console | http://localhost:8080 (profile: console) |
| Schema Registry | http://localhost:8081 |
| Postgres | localhost:5432 |
| Redis | localhost:6379 |

### Key Dashboards

- **System Overview**: Overall health, consumer lag, error rates
- **Trend Monitor**: Real-time trending topics, volume/acceleration graphs
- **Brief Status**: Latest briefs, generation success/failure
- **Data Freshness**: Consumer lag by service, staleness indicators

### Log Queries (Grafana/Loki)

```logql
# Errors in last hour
{service=~".+"} |= "error" | json | level="error"

# Specific service
{service="collector"} | json

# Brief generation failures
{service="brief"} |= "Brief generation failed" | json
```

---

## Common Scenarios

### Scenario 1: Brief Not Generated

**Symptoms**: No new brief in dashboard; `briefs_generated_total` metric flat.

**Investigation**:

1. Check if brief was triggered:
   ```bash
   # Check Kafka for SummaryRequest
   docker compose exec redpanda rpk topic consume summary.requests --num 1
   ```

2. Check data freshness (common cause):
   ```sql
   -- Check consumer lag
   SELECT * FROM consumer_lag WHERE consumer_group = 'trends-processor';
   ```

3. Check Brief service logs:
   ```bash
   docker compose logs brief --tail 100 | grep -i error
   ```

4. Check LLM budget:
   ```bash
   docker compose exec redis redis-cli HGETALL "budget:$(date +%Y-%m-%d)"
   ```

**Resolution**:

- If lag is high: Wait for consumers to catch up, or investigate slow dependency
- If budget exceeded: Wait for next day or increase `LLM_DAILY_BUDGET_USD`
- If LLM error: Check API key, rate limits, or LLM service status

---

### Scenario 2: High Consumer Lag

**Symptoms**: Grafana shows high lag for Persister or Trends; dashboards show stale data.

**Investigation**:

1. Check which service is lagging:
   ```sql
   SELECT consumer_group, SUM(lag_messages) as total_lag
   FROM consumer_lag
   GROUP BY consumer_group;
   ```

2. Check service health:
   ```bash
   curl http://localhost:3002/health  # Persister
   curl http://localhost:3003/health  # Trends
   ```

3. Check dependencies:
   ```bash
   # Postgres
   docker compose exec postgres pg_isready

   # Redis
   docker compose exec redis redis-cli PING
   ```

**Resolution**:

- If service is down: Restart it (`docker compose restart persister`)
- If Postgres is slow: Check for long-running queries, vacuum, or disk space
- If Redis is slow: Check memory usage, eviction rate
- If persistent: Consider adding more partitions and consumer instances

---

### Scenario 3: Duplicate Events

**Symptoms**: Same event appears multiple times in `raw_events`; trend scores seem inflated.

**Investigation**:

1. Check for duplicates in Postgres:
   ```sql
   SELECT event_id, COUNT(*) as cnt
   FROM raw_events
   WHERE fetched_at > NOW() - INTERVAL '1 day'
   GROUP BY event_id
   HAVING COUNT(*) > 1;
   ```

2. Check Collector checkpoints:
   ```bash
   docker compose exec collector cat /data/checkpoints.db
   # Or if SQLite:
   docker compose exec collector sqlite3 /data/checkpoints.db "SELECT * FROM checkpoints;"
   ```

**Resolution**:

- `raw_events` has UNIQUE constraint, so true duplicates shouldn't exist in Postgres
- If duplicates in Kafka: Persister handles via `ON CONFLICT DO NOTHING`
- If Collector keeps restarting: Check checkpoint volume mount
- For Trends dedup: The `dedup:{window}:{bucket}` Redis set should prevent double-counting

---

### Scenario 4: Missing Topics on Events

**Symptoms**: `topics` column is empty in `raw_events`; trend scores are zero.

**Investigation**:

1. Check if Collector is extracting topics:
   ```bash
   docker compose logs collector --tail 50 | grep -i topic
   ```

2. Check allowlist is loaded:
   ```bash
   docker compose exec collector cat /config/topics.allowlist.yaml
   ```

3. Check sample event in Kafka:
   ```bash
   docker compose exec redpanda rpk topic consume events.raw --num 1 | jq '.tags'
   ```

**Resolution**:

- If allowlist not found: Check `TOPICS_ALLOWLIST_PATH` env var and volume mount
- If regex errors: Check Collector startup logs for validation failures
- If tags in Kafka but not Postgres: Check Persister is mapping `tags` → `topics`

---

### Scenario 5: Redis Data Loss

**Symptoms**: Trend scores reset to zero; window counts are wrong.

**Investigation**:

1. Check Redis is running:
   ```bash
   docker compose exec redis redis-cli PING
   ```

2. Check Redis memory:
   ```bash
   docker compose exec redis redis-cli INFO memory
   ```

3. Check for evictions:
   ```bash
   docker compose exec redis redis-cli INFO stats | grep evicted
   ```

**Resolution**:

Redis data loss is **expected to be recoverable**:

1. Trends service replays from Kafka offset
2. After one window period (60m), counts stabilize
3. No manual intervention needed

If evictions are frequent:
- Increase `maxmemory` in Redis config
- Check for memory leaks (too many keys)

---

### Scenario 6: LLM API Failures

**Symptoms**: Briefs fail with "llm_error"; `brief_generation_failed_total` increasing.

**Investigation**:

1. Check Brief service logs:
   ```bash
   docker compose logs brief --tail 100 | grep -i "llm\|openai\|error"
   ```

2. Check LLM API status:
   - OpenAI: https://status.openai.com/
   - Anthropic: https://status.anthropic.com/

3. Check API key:
   ```bash
   # Verify key is set (don't log the actual value!)
   docker compose exec brief printenv OPENAI_API_KEY | wc -c
   ```

**Resolution**:

- If API down: Wait for recovery; briefs will be skipped (freshness check)
- If rate limited: Reduce `LLM_MAX_TOPICS_PER_BRIEF` or wait
- If key invalid: Update `.env` and restart Brief service
- If context too large: Evidence truncation should handle this; check logs

---

## Manual Operations

### Replay Data from Kafka

To reprocess events (e.g., after a bug fix):

```bash
# 1. Reset consumer offset to beginning
docker compose exec redpanda rpk group seek persister --to start --topics events.raw

# 2. Restart the consumer
docker compose restart persister

# 3. Monitor progress
watch -n 5 'docker compose exec redpanda rpk group describe persister'
```

**Warning**: This will reprocess ALL events. For large topics, consider:
- Resetting to a specific offset: `--to 12345`
- Resetting to a timestamp: `--to-timestamp 1707177600000`

---

### Manually Trigger a Brief

```bash
# Publish a SummaryRequest to Kafka
docker compose exec redpanda rpk topic produce summary.requests <<EOF
{
  "request_id": "manual-$(date +%s)",
  "requested_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "type": "DAILY",
  "topics": [
    {
      "topic": "aws.bedrock",
      "metrics": [{"window": "WINDOW_60M", "volume": 100, "score": 8.5}],
      "evidence": [{"title": "Test", "url": "https://example.com", "source": "RSS"}]
    }
  ]
}
EOF
```

---

### Clear Redis State

To force a clean slate (will cause temporary score inaccuracy):

```bash
# Clear all window state (dangerous!)
docker compose exec redis redis-cli FLUSHDB

# Or selectively:
docker compose exec redis redis-cli KEYS "window:*" | xargs docker compose exec redis redis-cli DEL
docker compose exec redis redis-cli KEYS "dedup:*" | xargs docker compose exec redis redis-cli DEL
```

---

### Run Retention Cleanup Manually

```bash
docker compose run --rm db npm run retention:cleanup
```

Or directly in Postgres:
```sql
-- Check what would be deleted
SELECT COUNT(*) FROM raw_events WHERE fetched_at < NOW() - INTERVAL '14 days';

-- Delete (be careful!)
DELETE FROM raw_events WHERE fetched_at < NOW() - INTERVAL '14 days';
```

---

### Export Data for Debugging

```bash
# Export recent events
docker compose exec postgres psql -U rising -d rising_intelligence -c \
  "COPY (SELECT * FROM raw_events WHERE fetched_at > NOW() - INTERVAL '1 hour') TO STDOUT WITH CSV HEADER" \
  > events_export.csv

# Export trend snapshots
docker compose exec postgres psql -U rising -d rising_intelligence -c \
  "COPY (SELECT * FROM trend_snapshots ORDER BY generated_at DESC LIMIT 100) TO STDOUT WITH CSV HEADER" \
  > snapshots_export.csv
```

---

## Recovery Procedures

### Full System Restart

```bash
# Graceful shutdown
docker compose down

# Start infrastructure first
docker compose up -d redpanda postgres redis grafana loki mimir tempo otel-collector

# Wait for health
sleep 30

# Start application services
docker compose up -d collector persister trends brief

# Verify
docker compose ps
curl http://localhost:3001  # Grafana
```

### Rebuild from Kafka

If Postgres is corrupted but Kafka has data:

```bash
# 1. Stop consumers
docker compose stop persister trends brief

# 2. Reset Postgres (DESTRUCTIVE!)
docker compose exec postgres psql -U rising -d rising_intelligence -c "TRUNCATE raw_events, trend_snapshots, brief_results CASCADE;"

# 3. Reset consumer offsets
docker compose exec redpanda rpk group seek persister --to start --topics events.raw
docker compose exec redpanda rpk group seek trends-processor --to start --topics events.raw

# 4. Restart consumers
docker compose start persister trends brief

# 5. Monitor rebuild progress
watch -n 10 'docker compose exec postgres psql -U rising -d rising_intelligence -c "SELECT COUNT(*) FROM raw_events;"'
```

### Disaster Recovery

If everything is lost, you need:

1. **Kafka data**: Events can be re-ingested from sources (but you lose history)
2. **Postgres data**: Derived from Kafka (rebuildable except for briefs)
3. **Checkpoints**: If lost, Collector re-ingests from beginning (duplicates handled)
4. **Redis**: Ephemeral; auto-recovers

**Minimum backup recommendation**: Back up Postgres daily; Kafka data has its own retention.

---

## Monitoring Checklist

Daily check (or automated alerts):

- [ ] Consumer lag < 1000 for all services
- [ ] Brief generated today (check `brief_results` table)
- [ ] No error spikes in Grafana
- [ ] Disk usage < 80% on all volumes
- [ ] Redis memory < 80% of maxmemory

Weekly check:

- [ ] Retention cleanup ran (check `last_cleanup_at`)
- [ ] No orphaned containers (`docker ps -a`)
- [ ] Grafana dashboards loading correctly
- [ ] Kafka topic sizes reasonable
