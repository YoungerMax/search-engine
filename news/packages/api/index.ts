import { sql, eq } from "drizzle-orm";
import { createDb, feeds, items } from "@news/db";
import { processFeed } from "@news/fetcher/processor";
import { fetchAndParse } from "@news/fetcher/parser";

const db = createDb();

// ── Search ────────────────────────────────────────────────────────────────────

async function searchItems(query: string | null, limit: number, offset: number) {
    const base = db.select({
        url: items.url,
        title: items.title,
        description: items.description,
        image: items.image,
        content: items.content,
        published: items.published,
        author: sql<string>`nullif(${items.author}, '')`,
        feed: {
            feedUrl: feeds.feedUrl,
            homeUrl: sql<string>`nullif(${feeds.homeUrl}, '')`,
            name: feeds.name,
            link: sql<string>`nullif(${feeds.link}, '')`,
            lastPublished: feeds.lastPublished,
            lastFetched: feeds.lastFetched,
            image: sql<string>`nullif(${feeds.image}, '')`,
            nextFetchAt: feeds.nextFetchAt,
            publishRatePerHour: feeds.publishRatePerHour,
        },
    }).from(items).leftJoin(feeds, eq(items.feedUrl, feeds.feedUrl));

    if (!query) {
        return base.limit(limit).offset(offset)
            .orderBy(sql`${items.published} DESC NULLS LAST`);
    }

    const tsQuery = query.trim().split(/\s+/).map(t => `${t}:*`).join(" & ");
    return base
        .where(sql`to_tsvector('english', coalesce(${items.title}, '') || ' ' || coalesce(${items.description}, '') || ' ' || coalesce(${items.content}, '')) @@ to_tsquery('english', ${tsQuery})`)
        .limit(limit).offset(offset)
        .orderBy(sql`${items.published} DESC NULLS LAST`);
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function json(data: unknown, status = 200) {
    return new Response(JSON.stringify(data), {
        status,
        headers: { "Content-Type": "application/json" },
    });
}

function intParam(params: URLSearchParams, key: string, fallback: number, min: number, max: number) {
    const raw = parseInt(params.get(key) ?? String(fallback), 10);
    return isNaN(raw) ? fallback : Math.max(min, Math.min(max, raw));
}

// ── Router ────────────────────────────────────────────────────────────────────

async function handler(req: Request): Promise<Response> {
    const url = new URL(req.url);

    if (url.pathname === "/" && req.method === "GET") {
        return new Response(Bun.file(new URL("./news.html", import.meta.url)), {
            headers: { "Content-Type": "text/html" },
        });
    }
    // GET /feeds
    if (url.pathname === "/feeds" && req.method === "GET") {
        return json(await db.select().from(feeds));
    }

    // POST /feeds?url=...  — subscribe to a new feed (immediate fetch)
    if (url.pathname === "/feeds" && req.method === "POST") {
        const feedUrl = url.searchParams.get("url");
        if (!feedUrl) return json({ error: "Missing url parameter" }, 400);

        const result = await processFeed(feedUrl, db);
        if (!result) return json({ error: "Failed to fetch or parse feed" }, 400);

        const [feed] = await db.select().from(feeds).where(eq(feeds.feedUrl, result.finalUrl));
        return json({ feed, itemsInserted: result.inserted });
    }

    // DELETE /feeds?url=...
    if (url.pathname === "/feeds" && req.method === "DELETE") {
        const feedUrl = url.searchParams.get("url");
        if (!feedUrl) return json({ error: "Missing url parameter" }, 400);
        await db.delete(feeds).where(eq(feeds.feedUrl, feedUrl));
        return json({ deleted: feedUrl });
    }

    // GET /items?q=&limit=&offset=
    if (url.pathname === "/items" && req.method === "GET") {
        const q = url.searchParams.get("q");
        const limit = intParam(url.searchParams, "limit", 20, 1, 100);
        const offset = intParam(url.searchParams, "offset", 0, 0, Infinity);
        return json(await searchItems(q, limit, offset));
    }

    if (req.method !== "GET" && req.method !== "POST" && req.method !== "DELETE") {
        return json({ error: "Method not allowed" }, 405);
    }

    return json({ error: "Not found" }, 404);
}

// ── Boot ──────────────────────────────────────────────────────────────────────

const port = parseInt(process.env.PORT ?? "3000", 10);
const server = Bun.serve({ port, fetch: handler });
console.log(`[api] Server listening on http://0.0.0.0:${server.port}`);