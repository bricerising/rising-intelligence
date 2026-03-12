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
  private readonly path: string;
  private readonly logger: Logger;

  private connection: CheckpointStoreConnection | null = null;

  constructor(path: string, logger: Logger) {
    this.path = path;
    this.logger = logger;
  }

  async initialize(): Promise<void> {
    if (this.connection) {
      throw new Error("Checkpoint store already initialized");
    }

    // Ensure directory exists
    const dir = dirname(this.path);
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
      this.logger.info({ dir }, "Created checkpoint directory");
    }

    // Open database
    const db = new Database(this.path);
    db.pragma("journal_mode = WAL");
    db.pragma("auto_vacuum = INCREMENTAL");
    db.pragma("synchronous = NORMAL");

    // Create tables
    db.exec(`
      CREATE TABLE IF NOT EXISTS checkpoints (
        source TEXT NOT NULL,
        checkpoint_key TEXT NOT NULL,
        checkpoint_value TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (source, checkpoint_key)
      )
    `);

    db.exec(`
      CREATE TABLE IF NOT EXISTS seen_events (
        source TEXT NOT NULL,
        event_id TEXT NOT NULL,
        seen_at TEXT NOT NULL,
        PRIMARY KEY (source, event_id)
      )
    `);

    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_seen_events_seen_at
      ON seen_events(seen_at)
    `);

    const statements: CheckpointStoreStatements = {
      getCheckpoint: db.prepare(`
        SELECT checkpoint_value
        FROM checkpoints
        WHERE source = ? AND checkpoint_key = ?
      `),
      setCheckpoint: db.prepare(`
        INSERT OR REPLACE INTO checkpoints (source, checkpoint_key, checkpoint_value, updated_at)
        VALUES (?, ?, ?, datetime('now'))
      `),
      hasSeen: db.prepare(`
        SELECT 1 as present
        FROM seen_events
        WHERE source = ? AND event_id = ?
      `),
      markSeen: db.prepare(`
        INSERT OR IGNORE INTO seen_events (source, event_id, seen_at)
        VALUES (?, ?, datetime('now'))
      `),
      cleanupSeen: db.prepare(`
        DELETE FROM seen_events
        WHERE seen_at < datetime('now', ?)
      `),
    };

    this.connection = {
      db,
      statements,
    };

    this.logger.info({ path: this.path }, "Checkpoint store initialized");
  }

  private getConnection(): CheckpointStoreConnection {
    if (!this.connection) {
      throw new Error("Checkpoint store not initialized");
    }
    return this.connection;
  }

  /**
   * Get a checkpoint value for a source/key.
   */
  getCheckpoint(source: string, checkpointKey: string): string | undefined {
    const row = this.getConnection().statements.getCheckpoint.get(source, checkpointKey) as
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
    this.getConnection().statements.setCheckpoint.run(
      source,
      checkpointKey,
      checkpointValue
    );
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
    const rows = this.getConnection().db
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
    const row = this.getConnection().statements.hasSeen.get(source, eventId) as
      | { present: number }
      | undefined;
    return row?.present === 1;
  }

  /**
   * Mark an event as seen (for dedup).
   */
  markSeen(source: Source, eventId: string): void {
    this.getConnection().statements.markSeen.run(source, eventId);
  }

  /**
   * Cleanup old seen events to prevent unbounded growth.
   * @param olderThan SQLite interval string, e.g., "-7 days"
   */
  cleanupSeen(olderThan: string = "-7 days"): number {
    const conn = this.getConnection();
    const result = conn.statements.cleanupSeen.run(olderThan);
    conn.db.pragma("incremental_vacuum");
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
    if (!this.connection) {
      return;
    }

    this.connection.db.close();
    this.connection = null;
    this.logger.info("Checkpoint store closed");
  }
}

interface CheckpointStoreStatements {
  getCheckpoint: Database.Statement;
  setCheckpoint: Database.Statement;
  hasSeen: Database.Statement;
  markSeen: Database.Statement;
  cleanupSeen: Database.Statement;
}

interface CheckpointStoreConnection {
  db: Database.Database;
  statements: CheckpointStoreStatements;
}
