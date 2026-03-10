FROM node:20-slim AS build

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends openssl \
  && rm -rf /var/lib/apt/lists/*

COPY package.json tsconfig.base.json tsconfig.json ./

COPY packages/shared/package.json packages/shared/tsconfig.json packages/shared/README.md ./packages/shared/
COPY packages/shared/src ./packages/shared/src
COPY packages/shared/contracts ./packages/shared/contracts

COPY packages/topics/package.json packages/topics/tsconfig.json ./packages/topics/
COPY packages/topics/src ./packages/topics/src

COPY packages/pipeline/package.json packages/pipeline/tsconfig.json ./packages/pipeline/
COPY packages/pipeline/src ./packages/pipeline/src

COPY packages/db/package.json packages/db/tsconfig.json ./packages/db/
COPY packages/db/src ./packages/db/src
COPY packages/db/scripts ./packages/db/scripts
COPY packages/db/prisma ./packages/db/prisma

COPY packages/ops-cli/package.json packages/ops-cli/tsconfig.json packages/ops-cli/README.md ./packages/ops-cli/
COPY packages/ops-cli/src ./packages/ops-cli/src
COPY infra/config/topics.allowlist.yaml ./infra/config/topics.allowlist.yaml

RUN npm install
RUN npm run build --workspace=@rising-intelligence/shared
RUN npm run build --workspace=@rising-intelligence/topics
RUN npm run build --workspace=@rising-intelligence/pipeline
RUN npm run build --workspace=@rising-intelligence/db
RUN npm run build --workspace=@rising-intelligence/ops-cli

FROM node:20-slim

WORKDIR /app
ENV NODE_ENV=production

RUN apt-get update \
  && apt-get install -y --no-install-recommends openssl curl ca-certificates gnupg \
  && mkdir -p /usr/share/keyrings \
  && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
    | gpg --dearmor -o /usr/share/keyrings/postgresql.gpg \
  && echo "deb [signed-by=/usr/share/keyrings/postgresql.gpg] https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
    > /etc/apt/sources.list.d/pgdg.list \
  && apt-get update \
  && apt-get install -y --no-install-recommends postgresql-client-16 \
  && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/packages ./packages
COPY --from=build /app/infra ./infra
COPY --from=build /app/package.json ./package.json

ENTRYPOINT ["node", "packages/ops-cli/dist/bin.js"]
