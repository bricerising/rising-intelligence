# Quickstart: Ops CLI

## Local

```bash
npm install
npm run build
./node_modules/.bin/riops --help
./node_modules/.bin/riops brief trigger --topic-key aws.bedrock --evidence-url https://example.com/bedrock --dry-run
```

## Docker Compose

The stack runs `ops-cli` on startup. Re-run manually:

```bash
docker compose run --rm ops-cli schema-registry publish-protos
```
