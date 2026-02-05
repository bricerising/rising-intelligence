# Quickstart: Persister Service

## Prerequisites

- Docker Compose stack running (`docker compose up -d`)
- Prisma migrations applied (`cd packages/db && npm run db:migrate:deploy`)

## Local Development

```bash
# Install dependencies
npm install

# Run in development mode
npm run dev

# Or build and run
npm run build
npm start
```

## Environment Variables

```bash
# Required
KAFKA_BROKERS=localhost:9092
DATABASE_URL=postgresql://rising:rising@localhost:5432/rising_intelligence
REDIS_URL=redis://localhost:6379

# Optional
KAFKA_CONSUMER_GROUP=persister
HEALTH_PORT=3002
LOG_LEVEL=info
```

## Verify It's Working

1. **Check consumer group**:
   ```bash
   docker exec -it rising-intelligence-redpanda-1 rpk group describe persister
   ```

2. **Check Postgres rows**:
   ```bash
   docker exec -it rising-intelligence-postgres-1 psql -U rising -d rising_intelligence -c "SELECT COUNT(*) FROM raw_events"
   ```

3. **Check Redis keys**:
   ```bash
   docker exec -it rising-intelligence-redis-1 redis-cli KEYS "seen:*" | head -10
   ```

4. **Check health**:
   ```bash
   curl http://localhost:3002/healthz
   curl http://localhost:3002/readyz
   ```

## Troubleshooting

### Consumer lag increasing

Check Postgres performance:
```bash
docker logs rising-intelligence-postgres-1 --tail 100
```

### Events not appearing in Postgres

Check persister logs:
```bash
docker logs rising-intelligence-persister-1 --tail 100
```

### Redis connection errors

Redis is optional; persister continues without it. Check Redis:
```bash
docker exec -it rising-intelligence-redis-1 redis-cli PING
```
