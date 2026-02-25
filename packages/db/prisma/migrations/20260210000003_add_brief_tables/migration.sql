-- CreateTable
CREATE TABLE "brief_trend_snapshots" (
    "id" BIGSERIAL NOT NULL,
    "generated_at" TIMESTAMPTZ NOT NULL,
    "window" "TrendWindow" NOT NULL,
    "snapshot" JSONB NOT NULL,

    CONSTRAINT "brief_trend_snapshots_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "brief_budget_tracking" (
    "date" DATE NOT NULL,
    "spent_usd" DECIMAL(10,4) NOT NULL DEFAULT 0,
    "budget_usd" DECIMAL(10,4) NOT NULL,
    "request_count" INTEGER NOT NULL DEFAULT 0,
    "updated_at" TIMESTAMPTZ NOT NULL,

    CONSTRAINT "brief_budget_tracking_pkey" PRIMARY KEY ("date")
);

-- CreateIndex
CREATE INDEX "idx_brief_trend_snapshots_window_generated" ON "brief_trend_snapshots"("window", "generated_at" DESC);
