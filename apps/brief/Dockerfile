# Brief Service Dockerfile
# Build from repository root: docker build -f apps/brief/Dockerfile .

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
COPY apps/brief/package.json ./apps/brief/

# Install dependencies
RUN npm ci --workspace=@rising-intelligence/brief

# =============================================================================
# Stage 2: Build
# =============================================================================
FROM node:20-alpine AS builder

WORKDIR /app

# Copy dependencies from deps stage
COPY --from=deps /app/node_modules ./node_modules
COPY --from=deps /app/packages/shared/node_modules ./packages/shared/node_modules
COPY --from=deps /app/packages/db/node_modules ./packages/db/node_modules
COPY --from=deps /app/apps/brief/node_modules ./apps/brief/node_modules

# Copy source code
COPY packages/shared ./packages/shared
COPY packages/db ./packages/db
COPY apps/brief ./apps/brief
COPY tsconfig.json tsconfig.base.json ./

# Build shared packages first
RUN npm run build --workspace=@rising-intelligence/shared
RUN npm run build --workspace=@rising-intelligence/db

# Generate Prisma client
RUN cd packages/db && npx prisma generate

# Build the service
RUN npm run build --workspace=@rising-intelligence/brief

# =============================================================================
# Stage 3: Production
# =============================================================================
FROM node:20-alpine AS runner

WORKDIR /app

# Add non-root user for security
RUN addgroup --system --gid 1001 nodejs && \
    adduser --system --uid 1001 brief

# Copy built artifacts
COPY --from=builder --chown=brief:nodejs /app/packages/shared/dist ./packages/shared/dist
COPY --from=builder --chown=brief:nodejs /app/packages/shared/package.json ./packages/shared/
COPY --from=builder --chown=brief:nodejs /app/packages/db/dist ./packages/db/dist
COPY --from=builder --chown=brief:nodejs /app/packages/db/package.json ./packages/db/
COPY --from=builder --chown=brief:nodejs /app/packages/db/node_modules/.prisma ./packages/db/node_modules/.prisma
COPY --from=builder --chown=brief:nodejs /app/apps/brief/dist ./apps/brief/dist
COPY --from=builder --chown=brief:nodejs /app/apps/brief/package.json ./apps/brief/

# Copy production dependencies only
COPY --from=deps --chown=brief:nodejs /app/node_modules ./node_modules

# Copy topics allowlist (for display names)
COPY --chown=brief:nodejs infra/config/topics.allowlist.yaml /config/topics.allowlist.yaml

USER brief

# Environment defaults
ENV NODE_ENV=production
ENV TOPICS_ALLOWLIST_PATH=/config/topics.allowlist.yaml
ENV LLM_DAILY_BUDGET_USD=1.00

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:3000/health || exit 1

EXPOSE 3000

CMD ["node", "apps/brief/dist/index.js"]
