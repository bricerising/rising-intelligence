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
COPY packages/db/package.json ./packages/db/
COPY apps/persister/package.json ./apps/persister/

# Install dependencies
RUN npm ci --workspace=@rising-intelligence/persister

# =============================================================================
# Stage 2: Build
# =============================================================================
FROM node:20-alpine AS builder

WORKDIR /app

# Copy dependencies from deps stage
COPY --from=deps /app/node_modules ./node_modules
COPY --from=deps /app/packages/shared/node_modules ./packages/shared/node_modules
COPY --from=deps /app/packages/db/node_modules ./packages/db/node_modules
COPY --from=deps /app/apps/persister/node_modules ./apps/persister/node_modules

# Copy source code
COPY packages/shared ./packages/shared
COPY packages/db ./packages/db
COPY apps/persister ./apps/persister
COPY tsconfig.json ./

# Build shared packages first
RUN npm run build --workspace=@rising-intelligence/shared
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
RUN addgroup --system --gid 1001 nodejs && \
    adduser --system --uid 1001 persister

# Copy built artifacts
COPY --from=builder --chown=persister:nodejs /app/packages/shared/dist ./packages/shared/dist
COPY --from=builder --chown=persister:nodejs /app/packages/shared/package.json ./packages/shared/
COPY --from=builder --chown=persister:nodejs /app/packages/db/dist ./packages/db/dist
COPY --from=builder --chown=persister:nodejs /app/packages/db/package.json ./packages/db/
COPY --from=builder --chown=persister:nodejs /app/packages/db/node_modules/.prisma ./packages/db/node_modules/.prisma
COPY --from=builder --chown=persister:nodejs /app/apps/persister/dist ./apps/persister/dist
COPY --from=builder --chown=persister:nodejs /app/apps/persister/package.json ./apps/persister/

# Copy production dependencies only
COPY --from=deps --chown=persister:nodejs /app/node_modules ./node_modules

USER persister

# Environment defaults
ENV NODE_ENV=production

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:3000/health || exit 1

EXPOSE 3000

CMD ["node", "apps/persister/dist/index.js"]
