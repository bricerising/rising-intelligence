# Trends Service Dockerfile
# Build from repository root: docker build -f apps/trends/Dockerfile .

# =============================================================================
# Stage 1: Dependencies
# =============================================================================
FROM node:20-alpine AS deps

WORKDIR /app

# Copy root package files
COPY package.json package-lock.json* ./

# Copy workspace package files
COPY packages/shared/package.json ./packages/shared/
COPY packages/db/package.json ./packages/db/
COPY apps/trends/package.json ./apps/trends/

# Install dependencies
RUN npm ci --workspace=@rising-intelligence/trends

# =============================================================================
# Stage 2: Build
# =============================================================================
FROM node:20-alpine AS builder

WORKDIR /app

# Copy dependencies from deps stage
COPY --from=deps /app/node_modules ./node_modules
COPY --from=deps /app/packages/shared/node_modules ./packages/shared/node_modules
COPY --from=deps /app/packages/db/node_modules ./packages/db/node_modules
COPY --from=deps /app/apps/trends/node_modules ./apps/trends/node_modules

# Copy source code
COPY packages/shared ./packages/shared
COPY packages/db ./packages/db
COPY apps/trends ./apps/trends
COPY tsconfig.json tsconfig.base.json ./

# Build shared packages first
RUN npm run build --workspace=@rising-intelligence/shared
RUN npm run build --workspace=@rising-intelligence/db

# Generate Prisma client
RUN cd packages/db && npx prisma generate

# Build the service
RUN npm run build --workspace=@rising-intelligence/trends

# =============================================================================
# Stage 3: Production
# =============================================================================
FROM node:20-alpine AS runner

WORKDIR /app

# Add non-root user for security
RUN addgroup --system --gid 1001 nodejs && \
    adduser --system --uid 1001 trends

# Copy built artifacts
COPY --from=builder --chown=trends:nodejs /app/packages/shared/dist ./packages/shared/dist
COPY --from=builder --chown=trends:nodejs /app/packages/shared/package.json ./packages/shared/
COPY --from=builder --chown=trends:nodejs /app/packages/db/dist ./packages/db/dist
COPY --from=builder --chown=trends:nodejs /app/packages/db/package.json ./packages/db/
COPY --from=builder --chown=trends:nodejs /app/packages/db/node_modules/.prisma ./packages/db/node_modules/.prisma
COPY --from=builder --chown=trends:nodejs /app/apps/trends/dist ./apps/trends/dist
COPY --from=builder --chown=trends:nodejs /app/apps/trends/package.json ./apps/trends/

# Copy production dependencies only
COPY --from=deps --chown=trends:nodejs /app/node_modules ./node_modules

# Copy topics allowlist (mounted at runtime typically, but include default)
COPY --chown=trends:nodejs infra/config/topics.allowlist.yaml /config/topics.allowlist.yaml

USER trends

# Environment defaults
ENV NODE_ENV=production
ENV TOPICS_ALLOWLIST_PATH=/config/topics.allowlist.yaml

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:3000/health || exit 1

EXPOSE 3000

CMD ["node", "apps/trends/dist/index.js"]
