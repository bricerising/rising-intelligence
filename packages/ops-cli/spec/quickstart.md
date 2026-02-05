# Quickstart: Ops CLI

## Local

```bash
npm install
npm run build
./node_modules/.bin/riops --help
```

## Docker Compose

The stack runs `ops-cli` on startup. Re-run manually:

```bash
docker compose run --rm ops-cli schema-registry publish-protos
```

