# Quickstart: Ops CLI

## Local

```bash
npm install
npm run build
./node_modules/.bin/riops --help
./node_modules/.bin/riops brief trigger --dry-run
./node_modules/.bin/riops brief trigger --lookback-days 7 --topic-globs "aws.*,ai.*" --dry-run
./node_modules/.bin/riops brief trigger --report-timezone America/New_York --report-start-at 2026-01-01T00:00:00-05:00 --report-end-at 2026-02-10T23:59:59-05:00 --dry-run
./node_modules/.bin/riops brief trigger --topic-key aws.bedrock --evidence-url https://example.com/bedrock --dry-run
./node_modules/.bin/riops topics list --counts --min-count 5
./node_modules/.bin/riops topics retag --dry-run
./node_modules/.bin/riops topics retag --source rss --limit 100 --dry-run
./node_modules/.bin/riops topics retag --all --batch-size 500
```

## Docker Compose

The stack runs `ops-cli` on startup. Re-run manually:

```bash
docker compose run --rm ops-cli schema-registry publish-protos
```
