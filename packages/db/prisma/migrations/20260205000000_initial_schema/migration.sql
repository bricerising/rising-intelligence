-- Rising Intelligence Initial Schema
-- Generated: 2026-02-05
--
-- This migration creates the initial database schema including:
-- - raw_events: Queryable event archive
-- - trend_snapshots: Trend metrics over time
-- - brief_results: LLM-generated briefs
-- - consumer_lag: Kafka consumer tracking
-- - retention_policies: Cleanup configuration
-- - discovery_candidates: Emerging topic tracking

-- CreateEnum
CREATE TYPE "Source" AS ENUM ('rss', 'news', 'hackernews', 'reddit', 'github', 'bluesky', 'mastodon');

-- CreateEnum
CREATE TYPE "TrendWindow" AS ENUM ('WINDOW_15M', 'WINDOW_60M', 'WINDOW_24H');

-- CreateEnum
CREATE TYPE "BriefStatus" AS ENUM ('success', 'failure');

-- CreateEnum
CREATE TYPE "DiscoveryStatus" AS ENUM ('pending', 'added', 'ignored');

-- CreateTable
CREATE TABLE "raw_events" (
    "id" BIGSERIAL NOT NULL,
    "event_id" TEXT NOT NULL,
    "source" "Source" NOT NULL,
    "fetched_at" TIMESTAMPTZ NOT NULL,
    "published_at" TIMESTAMPTZ,
    "url" TEXT,
    "title" TEXT,
    "text" TEXT NOT NULL,
    "author_id" TEXT,
    "author_handle" TEXT,
    "author_display_name" TEXT,
    "engagement_score" INTEGER,
    "engagement_comments" INTEGER,
    "engagement_likes" INTEGER,
    "engagement_shares" INTEGER,
    "lang" TEXT,
    "tags" TEXT[],
    "extracted_hashtags" TEXT[],
    "extracted_urls" TEXT[],
    "topics" TEXT[],
    "source_meta" JSONB,

    CONSTRAINT "raw_events_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "trend_snapshots" (
    "id" BIGSERIAL NOT NULL,
    "generated_at" TIMESTAMPTZ NOT NULL,
    "window" "TrendWindow" NOT NULL,
    "snapshot" JSONB NOT NULL,

    CONSTRAINT "trend_snapshots_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "brief_results" (
    "request_id" TEXT NOT NULL,
    "produced_at" TIMESTAMPTZ NOT NULL,
    "status" "BriefStatus" NOT NULL,
    "result" JSONB NOT NULL,

    CONSTRAINT "brief_results_pkey" PRIMARY KEY ("request_id")
);

-- CreateTable
CREATE TABLE "consumer_lag" (
    "consumer_group" TEXT NOT NULL,
    "topic" TEXT NOT NULL,
    "partition" INTEGER NOT NULL,
    "current_offset" BIGINT NOT NULL,
    "latest_offset" BIGINT NOT NULL,
    "lag_messages" BIGINT NOT NULL,
    "updated_at" TIMESTAMPTZ NOT NULL,

    CONSTRAINT "consumer_lag_pkey" PRIMARY KEY ("consumer_group","topic","partition")
);

-- CreateTable
CREATE TABLE "retention_policies" (
    "table_name" TEXT NOT NULL,
    "retention_days" INTEGER NOT NULL,
    "enabled" BOOLEAN NOT NULL DEFAULT true,
    "last_cleanup_at" TIMESTAMPTZ,

    CONSTRAINT "retention_policies_pkey" PRIMARY KEY ("table_name")
);

-- CreateTable
CREATE TABLE "discovery_candidates" (
    "term" TEXT NOT NULL,
    "first_seen_at" TIMESTAMPTZ NOT NULL,
    "last_seen_at" TIMESTAMPTZ NOT NULL,
    "volume_24h" INTEGER NOT NULL,
    "peak_acceleration" DOUBLE PRECISION NOT NULL,
    "sample_urls" TEXT[],
    "sample_event_ids" TEXT[],
    "sources" "Source"[],
    "status" "DiscoveryStatus" NOT NULL DEFAULT 'pending',
    "added_to_allowlist_at" TIMESTAMPTZ,
    "ignored_at" TIMESTAMPTZ,
    "notes" TEXT,

    CONSTRAINT "discovery_candidates_pkey" PRIMARY KEY ("term")
);

-- CreateIndex
CREATE UNIQUE INDEX "raw_events_event_id_key" ON "raw_events"("event_id");

-- CreateIndex
CREATE INDEX "idx_raw_events_source_fetched" ON "raw_events"("source", "fetched_at" DESC);

-- CreateIndex
CREATE INDEX "idx_raw_events_fetched" ON "raw_events"("fetched_at" DESC);

-- CreateIndex
CREATE INDEX "idx_raw_events_published" ON "raw_events"("published_at" DESC);

-- CreateIndex
CREATE INDEX "idx_raw_events_topics" ON "raw_events" USING GIN ("topics");

-- CreateIndex
CREATE INDEX "idx_trend_snapshots_generated" ON "trend_snapshots"("generated_at" DESC);

-- CreateIndex
CREATE INDEX "idx_trend_snapshots_window_generated" ON "trend_snapshots"("window", "generated_at" DESC);

-- CreateIndex
CREATE INDEX "idx_brief_results_produced" ON "brief_results"("produced_at" DESC);

-- CreateIndex
CREATE INDEX "idx_discovery_candidates_status_volume" ON "discovery_candidates"("status", "volume_24h" DESC);

-- CreateIndex
CREATE INDEX "idx_discovery_candidates_last_seen" ON "discovery_candidates"("last_seen_at" DESC);

-- Seed retention policies
INSERT INTO "retention_policies" ("table_name", "retention_days", "enabled") VALUES
    ('raw_events', 14, true),
    ('trend_snapshots', 90, true),
    ('brief_results', 180, true),
    ('discovery_candidates', 30, true);
