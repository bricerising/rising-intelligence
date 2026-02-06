import { Source } from "@rising-intelligence/db";

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
