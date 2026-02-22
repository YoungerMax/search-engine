import { DOMParser } from "linkedom";
import type { feeds, items } from "@news/db";
import he from "he";

type FeedInsert = typeof feeds.$inferInsert;
type ItemInsert = typeof items.$inferInsert;

type FeedType = "rss" | "atom";

interface ImageCandidate {
    url: string;
    width: number;
    height: number;
}

export function normalizeText(input: string): string {
  if (!input) return "";

  // Decode HTML entities (&amp; &#8217; etc.)
  let text = he.decode(input);

  // Strip HTML tags by repeatedly removing tag-like content
  // Simple but effective for RSS descriptions
  text = text.replace(/<[^>]*>/g, " ");

  // Decode again in case there were encoded entities inside tags
  text = he.decode(text);

  // Normalize whitespace
  text = text.replace(/\s+/g, " ").trim();

  return text;
}

function detectFeedType(xml: string): FeedType {
    if (xml.includes("<feed") && xml.includes('xmlns="http://www.w3.org/2005/Atom"')) {
        return "atom";
    }
    return "rss";
}

function text(el: Element | null | undefined): string {
    return el?.textContent?.trim() ?? "";
}

function parseDate(dateStr: string): Date | null {
    if (!dateStr) return null;
    const d = new Date(dateStr);
    return isNaN(d.getTime()) ? null : d;
}

function parseNum(str: string | null): number {
    const n = parseInt(str ?? "", 10);
    return isNaN(n) ? 0 : n;
}

function bestImage(candidates: ImageCandidate[]): string | null {
    if (!candidates.length) return null;

    const score = (img: ImageCandidate): number => {
        const w = img.width ?? 0;
        const h = img.height ?? 0;

        // Prefer area when both exist
        if (w && h) return w * h;

        // Fallback if only one dimension exists
        return Math.max(w, h);
    };

    return candidates
        .slice() // avoid mutating original array
        .sort((a, b) => score(b) - score(a))[0]?.url ?? null;
}

function rssImages(el: Element): ImageCandidate[] {
    const out: ImageCandidate[] = [];
    for (const enc of el.querySelectorAll('enclosure[type^="image"]')) {
        const url = enc.getAttribute("url");
        if (url) out.push({ url, width: parseNum(enc.getAttribute("width")), height: parseNum(enc.getAttribute("height")) });
    }
    for (const mc of el.querySelectorAll("media\\:content, media\\:thumbnail")) {
        const url = mc.getAttribute("url");
        if (url) out.push({ url, width: parseNum(mc.getAttribute("width")), height: parseNum(mc.getAttribute("height")) });
    }
    return out;
}

function atomImages(el: Element): ImageCandidate[] {
    const out: ImageCandidate[] = [];
    for (const mt of el.querySelectorAll("media\\:thumbnail")) {
        const url = mt.getAttribute("url");
        if (url) out.push({ url, width: parseNum(mt.getAttribute("width")), height: parseNum(mt.getAttribute("height")) });
    }
    for (const mc of el.querySelectorAll('media\\:content[medium="image"]')) {
        const url = mc.getAttribute("url");
        if (url) out.push({ url, width: parseNum(mc.getAttribute("width")), height: parseNum(mc.getAttribute("height")) });
    }
    return out;
}

export interface ParsedFeed {
    feedInfo: Partial<FeedInsert>;
    items: Partial<ItemInsert>[];
    finalUrl: string;
}

interface ParsedResult {
    feedInfo: Partial<FeedInsert>;
    items: Partial<ItemInsert>[];
}

function parseRss(xml: string, feedUrl: string): ParsedResult {
    const doc = new DOMParser().parseFromString(xml, "text/xml");
    const ch = doc.querySelector("channel");

    const feedInfo: Partial<FeedInsert> = {
        feedUrl,
        name: text(ch?.querySelector("title")),
        homeUrl: text(ch?.querySelector("link")) || undefined,
        image: text(ch?.querySelector("image url")) || text(ch?.querySelector("image")) || undefined,
    };

    const parsedItems: Partial<ItemInsert>[] = [];
    for (const el of doc.querySelectorAll("item")) {
        parsedItems.push({
            url: text(el.querySelector("link")),
            title: normalizeText(text(el.querySelector("title"))),
            description: normalizeText(text(el.querySelector("description"))),
            content: text(el.querySelector("content\\:encoded")) || undefined,
            author: text(el.querySelector("author")) || text(el.querySelector("dc\\:creator")),
            published: parseDate(text(el.querySelector("pubDate"))) ?? undefined,
            image: bestImage(rssImages(el)) ?? undefined,
        });
    }

    return { feedInfo, items: parsedItems };
}

function parseAtom(xml: string, feedUrl: string): ParsedResult {
    const doc = new DOMParser().parseFromString(xml, "text/xml");
    const feedEl = doc.querySelector("feed");
    const linkEl = feedEl?.querySelector('link[rel="alternate"]') ?? feedEl?.querySelector("link");

    const feedInfo: Partial<FeedInsert> = {
        feedUrl,
        name: text(feedEl?.querySelector("title")),
        homeUrl: linkEl?.getAttribute("href") ?? undefined,
        image: text(feedEl?.querySelector("icon")) || text(feedEl?.querySelector("logo")) || undefined,
    };

    const parsedItems: Partial<ItemInsert>[] = [];
    for (const el of doc.querySelectorAll("entry")) {
        const entryLink = el.querySelector('link[rel="alternate"]') ?? el.querySelector("link");
        parsedItems.push({
            url: entryLink?.getAttribute("href") ?? "",
            title: normalizeText(text(el.querySelector("title"))),
            description: normalizeText(text(el.querySelector("summary"))),
            content: text(el.querySelector("content")) || undefined,
            author: text(el.querySelector("author name")),
            published: parseDate(text(el.querySelector("published")) || text(el.querySelector("updated"))) ?? undefined,
            image: bestImage(atomImages(el)) ?? undefined,
        });
    }

    return { feedInfo, items: parsedItems };
}

export async function fetchAndParse(feedUrl: string): Promise<ParsedFeed | null> {
    try {
        const res = await fetch(feedUrl);
        if (!res.ok) {
            console.error(`[parser] Failed to fetch ${feedUrl}: HTTP ${res.status}`);
            return null;
        }
        const finalUrl = res.url;
        const xml = await res.text();
        const parsed = detectFeedType(xml) === "atom" ? parseAtom(xml, finalUrl) : parseRss(xml, finalUrl);
        return { ...parsed, finalUrl };
    } catch (err) {
        console.error(`[parser] Error fetching ${feedUrl}:`, err);
        return null;
    }
}