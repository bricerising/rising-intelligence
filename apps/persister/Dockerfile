# Persister Service Dockerfile
# Build from repository root: docker build -f apps/persister/Dockerfile .

# =============================================================================
# Stage 1: Dependencies
# =============================================================================
FROM node:20-alpine AS deps

WORKDIR /app

# Copy root package files
COPY package.json package-lock.json* ./

# Copy workspace package files
COPY packages/shared/package.json ./packages/shared/
COPY packages/pipeline/package.json ./packages/pipeline/
COPY packages/db/package.json ./packages/db/
COPY apps/persister/package.json ./apps/persister/

# Install dependencies for all workspaces needed by this service build
RUN npm ci \
    --workspace=@rising-intelligence/shared \
    --workspace=@rising-intelligence/pipeline \
    --workspace=@rising-intelligence/db \
    --workspace=@rising-intelligence/persister

# =============================================================================
# Stage 2: Build
# =============================================================================
FROM node:20-alpine AS builder

WORKDIR /app

RUN apk add --no-cache openssl

# Copy root package files for workspace commands
COPY package.json package-lock.json* ./

# Copy dependencies from deps stage
COPY --from=deps /app/node_modules ./node_modules

# Copy source code
COPY packages/shared ./packages/shared
COPY packages/pipeline ./packages/pipeline
COPY packages/db ./packages/db
COPY apps/persister ./apps/persister
COPY tsconfig.json tsconfig.base.json ./

# Build shared packages first
RUN npm run build --workspace=@rising-intelligence/shared
RUN npm run build --workspace=@rising-intelligence/pipeline
RUN npm run build --workspace=@rising-intelligence/db

# Generate Prisma client
RUN cd packages/db && npx prisma generate

# Build the service
RUN npm run build --workspace=@rising-intelligence/persister

# =============================================================================
# Stage 3: Production
# =============================================================================
FROM node:20-alpine AS runner

WORKDIR /app

# Add non-root user for security
RUN apk add --no-cache openssl && \
    addgroup --system --gid 1001 nodejs && \
    adduser --system --uid 1001 persister

# Copy built artifacts
COPY --from=builder --chown=persister:nodejs /app/packages/shared/dist ./packages/shared/dist
COPY --from=builder --chown=persister:nodejs /app/packages/shared/package.json ./packages/shared/
COPY --from=builder --chown=persister:nodejs /app/packages/pipeline/dist ./packages/pipeline/dist
COPY --from=builder --chown=persister:nodejs /app/packages/pipeline/package.json ./packages/pipeline/
COPY --from=builder --chown=persister:nodejs /app/packages/db/dist ./packages/db/dist
COPY --from=builder --chown=persister:nodejs /app/packages/db/package.json ./packages/db/
COPY --from=builder --chown=persister:nodejs /app/apps/persister/dist ./apps/persister/dist
COPY --from=builder --chown=persister:nodejs /app/apps/persister/package.json ./apps/persister/

# Copy production dependencies only
COPY --from=builder --chown=persister:nodejs /app/node_modules ./node_modules

USER persister

# Environment defaults
ENV NODE_ENV=production

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:3000/health || exit 1

EXPOSE 3000

CMD ["node", "apps/persister/dist/index.js"]
