import Database from "better-sqlite3";
import { mkdirSync, existsSync } from "node:fs";
import { dirname } from "node:path";
import type { Logger } from "pino";
import type { Source } from "./types.js";

/**
 * SQLite-based checkpoint and seen-event storage.
 * Provides local persistence for:
 * - Checkpoints: per-source cursors to resume from after restart
 * - Seen cache: event IDs to prevent duplicate processing
 */
export class CheckpointStore {
  private db: Database.Database | null = null;
  private readonly path: string;
  private readonly logger: Logger;

  // Prepared statements (cached for performance)
  private stmtGetCheckpoint: Database.Statement | null = null;
  private stmtSetCheckpoint: Database.Statement | null = null;
  private stmtHasSeen: Database.Statement | null = null;
  private stmtMarkSeen: Database.Statement | null = null;
  private stmtCleanupSeen: Database.Statement | null = null;

  constructor(path: string, logger: Logger) {
    this.path = path;
    this.logger = logger;
  }

  async initialize(): Promise<void> {
    // Ensure directory exists
    const dir = dirname(this.path);
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
      this.logger.info({ dir }, "Created checkpoint directory");
    }

    // Open database
    this.db = new Database(this.path);
    this.db.pragma("journal_mode = WAL");
    this.db.pragma("synchronous = NORMAL");

    // Create tables
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS checkpoints (
        source TEXT NOT NULL,
        checkpoint_key TEXT NOT NULL,
        checkpoint_value TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (source, checkpoint_key)
      )
    `);

    this.db.exec(`
      CREATE TABLE IF NOT EXISTS seen_events (
        source TEXT NOT NULL,
        event_id TEXT NOT NULL,
        seen_at TEXT NOT NULL,
        PRIMARY KEY (source, event_id)
      )
    `);

    this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_seen_events_seen_at
      ON seen_events(seen_at)
    `);

    // Prepare statements
    this.stmtGetCheckpoint = this.db.prepare(`
      SELECT checkpoint_value
      FROM checkpoints
      WHERE source = ? AND checkpoint_key = ?
    `);

    this.stmtSetCheckpoint = this.db.prepare(`
      INSERT OR REPLACE INTO checkpoints (source, checkpoint_key, checkpoint_value, updated_at)
      VALUES (?, ?, ?, datetime('now'))
    `);

    this.stmtHasSeen = this.db.prepare(`
      SELECT 1 as present
      FROM seen_events
      WHERE source = ? AND event_id = ?
    `);

    this.stmtMarkSeen = this.db.prepare(`
      INSERT OR IGNORE INTO seen_events (source, event_id, seen_at)
      VALUES (?, ?, datetime('now'))
    `);

    this.stmtCleanupSeen = this.db.prepare(`
      DELETE FROM seen_events
      WHERE seen_at < datetime('now', ?)
    `);

    this.logger.info({ path: this.path }, "Checkpoint store initialized");
  }

  /**
   * Get a checkpoint value for a source/key.
   */
  getCheckpoint(source: string, checkpointKey: string): string | undefined {
    if (!this.stmtGetCheckpoint) {
      throw new Error("Checkpoint store not initialized");
    }

    const row = this.stmtGetCheckpoint.get(source, checkpointKey) as
      | { checkpoint_value: string }
      | undefined;
    return row?.checkpoint_value;
  }

  /**
   * Set a checkpoint value for a source/key.
   */
  setCheckpoint(
    source: string,
    checkpointKey: string,
    checkpointValue: string
  ): void {
    if (!this.stmtSetCheckpoint) {
      throw new Error("Checkpoint store not initialized");
    }

    this.stmtSetCheckpoint.run(source, checkpointKey, checkpointValue);
    this.logger.debug(
      { source, checkpointKey, checkpointValue },
      "Checkpoint updated"
    );
  }

  /**
   * Get all checkpoints for a source prefix.
   */
  listCheckpoints(
    sourcePrefix: string
  ): Record<string, Record<string, string>> {
    if (!this.db) {
      throw new Error("Checkpoint store not initialized");
    }

    const rows = this.db
      .prepare(
        `
        SELECT source, checkpoint_key, checkpoint_value
        FROM checkpoints
        WHERE source LIKE ?
      `
      )
      .all(`${sourcePrefix}%`) as Array<{
      source: string;
      checkpoint_key: string;
      checkpoint_value: string;
    }>;

    const result: Record<string, Record<string, string>> = {};
    for (const row of rows) {
      result[row.source] ??= {};
      result[row.source][row.checkpoint_key] = row.checkpoint_value;
    }
    return result;
  }

  /**
   * Check if an event has been seen (dedup check).
   */
  hasSeen(source: Source, eventId: string): boolean {
    if (!this.stmtHasSeen) {
      throw new Error("Checkpoint store not initialized");
    }

    const row = this.stmtHasSeen.get(source, eventId) as
      | { present: number }
      | undefined;
    return row?.present === 1;
  }

  /**
   * Mark an event as seen (for dedup).
   */
  markSeen(source: Source, eventId: string): void {
    if (!this.stmtMarkSeen) {
      throw new Error("Checkpoint store not initialized");
    }

    this.stmtMarkSeen.run(source, eventId);
  }

  /**
   * Cleanup old seen events to prevent unbounded growth.
   * @param olderThan SQLite interval string, e.g., "-7 days"
   */
  cleanupSeen(olderThan: string = "-7 days"): number {
    if (!this.stmtCleanupSeen) {
      throw new Error("Checkpoint store not initialized");
    }

    const result = this.stmtCleanupSeen.run(olderThan);
    this.logger.info(
      { deleted: result.changes, olderThan },
      "Cleaned up old seen events"
    );
    return result.changes;
  }

  /**
   * Close the database connection.
   */
  close(): void {
    if (this.db) {
      this.db.close();
      this.db = null;
      this.logger.info("Checkpoint store closed");
    }
  }
}
