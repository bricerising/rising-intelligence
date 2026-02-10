FROM node:20-slim AS build

WORKDIR /app

COPY package.json tsconfig.base.json tsconfig.json ./

COPY packages/shared/package.json packages/shared/tsconfig.json packages/shared/README.md ./packages/shared/
COPY packages/shared/src ./packages/shared/src
COPY packages/shared/contracts ./packages/shared/contracts

COPY packages/db/package.json packages/db/tsconfig.json ./packages/db/
COPY packages/db/src ./packages/db/src
COPY packages/db/scripts ./packages/db/scripts
COPY packages/db/prisma ./packages/db/prisma

COPY packages/ops-cli/package.json packages/ops-cli/tsconfig.json packages/ops-cli/README.md ./packages/ops-cli/
COPY packages/ops-cli/src ./packages/ops-cli/src

RUN npm install
RUN npm run build --workspace=@rising-intelligence/shared
RUN npm run build --workspace=@rising-intelligence/db
RUN npm run build --workspace=@rising-intelligence/ops-cli

FROM node:20-slim

WORKDIR /app
ENV NODE_ENV=production

COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/packages ./packages
COPY --from=build /app/package.json ./package.json

ENTRYPOINT ["node", "packages/ops-cli/dist/bin.js"]
