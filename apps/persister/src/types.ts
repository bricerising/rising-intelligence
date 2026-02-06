import { Source } from "@rising-intelligence/db";

export interface WireAuthor {
  id?: string;
  handle?: string;
  display_name?: string;
}

export interface WireEngagement {
  score?: number;
  comments?: number;
  likes?: number;
  shares?: number;
}

export interface WireExtracted {
  hashtags?: string[];
  urls?: string[];
}

export interface RawEventWireEnvelope {
  event_id: string;
  source: number | string;
  fetched_at: string;
  published_at?: string;
  url?: string;
  title?: string;
  text: string;
  author?: WireAuthor;
  engagement?: WireEngagement;
  lang?: string;
  tags?: string[];
  extracted?: WireExtracted;
  source_meta_json?: string;
}

export interface ParsedRawEvent {
  eventId: string;
  source: Source;
  fetchedAt: Date;
  publishedAt: Date | null;
  url: string | null;
  title: string | null;
  text: string;
  authorId: string | null;
  authorHandle: string | null;
  authorDisplayName: string | null;
  engagementScore: number | null;
  engagementComments: number | null;
  engagementLikes: number | null;
  engagementShares: number | null;
  lang: string | null;
  tags: string[];
  extractedHashtags: string[];
  extractedUrls: string[];
  sourceMeta: Record<string, unknown> | null;
}
