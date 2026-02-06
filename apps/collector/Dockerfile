# Collector Service Dockerfile
# Build from repository root: docker build -f apps/collector/Dockerfile .

# =============================================================================
# Stage 1: Dependencies
# =============================================================================
FROM node:20-alpine AS deps

WORKDIR /app

# Copy root package files
COPY package.json package-lock.json* ./

# Copy workspace package files
COPY packages/shared/package.json ./packages/shared/
COPY apps/collector/package.json ./apps/collector/

# Install dependencies
RUN npm ci --workspace=@rising-intelligence/collector

# =============================================================================
# Stage 2: Build
# =============================================================================
FROM node:20-alpine AS builder

WORKDIR /app

# Copy dependencies from deps stage
COPY --from=deps /app/node_modules ./node_modules
COPY --from=deps /app/packages/shared/node_modules ./packages/shared/node_modules
COPY --from=deps /app/apps/collector/node_modules ./apps/collector/node_modules

# Copy source code
COPY packages/shared ./packages/shared
COPY apps/collector ./apps/collector
COPY tsconfig.json tsconfig.base.json ./

# Build shared packages first
RUN npm run build --workspace=@rising-intelligence/shared

# Build the service
RUN npm run build --workspace=@rising-intelligence/collector

# =============================================================================
# Stage 3: Production
# =============================================================================
FROM node:20-alpine AS runner

WORKDIR /app

# Add non-root user for security
RUN addgroup --system --gid 1001 nodejs && \
    adduser --system --uid 1001 collector

# Copy built artifacts
COPY --from=builder --chown=collector:nodejs /app/packages/shared/dist ./packages/shared/dist
COPY --from=builder --chown=collector:nodejs /app/packages/shared/package.json ./packages/shared/
COPY --from=builder --chown=collector:nodejs /app/apps/collector/dist ./apps/collector/dist
COPY --from=builder --chown=collector:nodejs /app/apps/collector/package.json ./apps/collector/

# Copy production dependencies only
COPY --from=deps --chown=collector:nodejs /app/node_modules ./node_modules

# Copy config files (mounted at runtime typically, but include defaults)
COPY --chown=collector:nodejs infra/config/topics.allowlist.yaml /config/topics.allowlist.yaml
COPY --chown=collector:nodejs infra/config/feeds.yaml /config/feeds.yaml

# Create checkpoint directory
RUN mkdir -p /data && chown collector:nodejs /data

USER collector

# Environment defaults
ENV NODE_ENV=production
ENV CHECKPOINT_PATH=/data/checkpoints.db
ENV TOPICS_ALLOWLIST_PATH=/config/topics.allowlist.yaml
ENV FEEDS_CONFIG_PATH=/config/feeds.yaml

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:3000/health || exit 1

EXPOSE 3000

CMD ["node", "apps/collector/dist/index.js"]
