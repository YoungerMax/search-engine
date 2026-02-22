import { lte, isNull, or, sql, asc } from "drizzle-orm";
import { createDb, feeds } from "@news/db";
import { processFeed } from "./processor.ts";

/**
 * How often the scheduler wakes up to check for feeds that are due.
 * It will sleep until the nearest nextFetchAt if that's sooner.
 */
const TICK_MS = 60_000; // 1 minute

const db = createDb();

async function getDueFeeds(): Promise<{ feedUrl: string }[]> {
    const now = new Date();
    return db
        .select({ feedUrl: feeds.feedUrl })
        .from(feeds)
        .where(
            or(
                isNull(feeds.nextFetchAt),
                lte(feeds.nextFetchAt, now),
            )
        )
        .orderBy(asc(feeds.nextFetchAt));
}

async function getNextWakeMs(): Promise<number> {
    const [next] = await db
        .select({ nextFetchAt: feeds.nextFetchAt })
        .from(feeds)
        .where(sql`${feeds.nextFetchAt} > now()`)
        .orderBy(asc(feeds.nextFetchAt))
        .limit(1);

    if (!next?.nextFetchAt) return TICK_MS;
    const msUntil = next.nextFetchAt.getTime() - Date.now();
    return Math.max(0, Math.min(msUntil, TICK_MS));
}

async function tick() {
    const due = await getDueFeeds();
    if (due.length === 0) {
        console.log(`[scheduler] No feeds due.`);
        return;
    }

    console.log(`[scheduler] ${due.length} feed(s) due — processing...`);

    // Process concurrently but cap concurrency to avoid thundering herd
    const CONCURRENCY = 5;
    for (let i = 0; i < due.length; i += CONCURRENCY) {
        const batch = due.slice(i, i + CONCURRENCY);
        await Promise.allSettled(batch.map(f => processFeed(f.feedUrl, db)));
    }
}

async function run() {
    console.log("[scheduler] Fetcher started.");

    while (true) {
        try {
            await tick();
        } catch (err) {
            console.error("[scheduler] Tick error:", err);
        }

        const sleepMs = await getNextWakeMs();
        console.log(`[scheduler] Sleeping ${Math.round(sleepMs / 1000)}s until next wake.`);
        await Bun.sleep(sleepMs);
    }
}

run();