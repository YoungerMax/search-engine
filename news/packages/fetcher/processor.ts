import { eq, sql } from "drizzle-orm";
import { createDb, feeds, items } from "@news/db";
import { fetchAndParse } from "./parser.ts";
import { downloadImageAsBase64 } from "./images.ts";
import { computeNextFetch } from "./scheduler.ts";

function nullIfEmpty(val: string | null | undefined): string | null {
    return val && val.trim() ? val : null;
}

export async function processFeed(feedUrl: string, db = createDb()): Promise<{ inserted: number; finalUrl: string } | null> {
    const parsed = await fetchAndParse(feedUrl);
    if (!parsed) return null;

    const { feedInfo, items: parsedItems, finalUrl } = parsed;

    const [existing] = await db
        .select({ rate: feeds.publishRatePerHour })
        .from(feeds)
        .where(eq(feeds.feedUrl, finalUrl));

    // Compute next fetch schedule from item timestamps
    const { nextFetchAt, newRatePerHour } = computeNextFetch(
        parsedItems.map(i => i.published ?? null),
        existing?.rate ?? null,
    );

    await db.insert(feeds).values({
        feedUrl: finalUrl,
        name: nullIfEmpty(feedInfo.name),
        homeUrl: nullIfEmpty(feedInfo.homeUrl),
        image: nullIfEmpty(feedInfo.image),
        lastFetched: new Date(),
        publishRatePerHour: newRatePerHour,
        nextFetchAt,
    }).onConflictDoUpdate({
        target: feeds.feedUrl,
        set: {
            name: nullIfEmpty(feedInfo.name),
            homeUrl: nullIfEmpty(feedInfo.homeUrl),
            image: nullIfEmpty(feedInfo.image),
            lastFetched: new Date(),
            publishRatePerHour: newRatePerHour,
            nextFetchAt,
        },
    });

    let inserted = 0;
    for (const item of parsedItems) {
        if (!item.url) continue;
        try {
            const image = item.image ? await downloadImageAsBase64(item.image) ?? undefined : undefined;
            const result = await db.insert(items).values({
                url: item.url,
                feedUrl: finalUrl,
                title: nullIfEmpty(item.title),
                description: nullIfEmpty(item.description),
                content: nullIfEmpty(item.content),
                published: item.published,
                author: nullIfEmpty(item.author),
                image,
            }).onConflictDoNothing();
            if (result.length) inserted++;
        } catch (err) {
            console.error(`[processor] Failed to insert ${item.url}:`, err);
        }
    }

    console.log(`[processor] ${finalUrl} → ${inserted} new items, next fetch at ${nextFetchAt.toISOString()} (λ=${newRatePerHour?.toFixed(4) ?? "?"} items/hr)`);
    return { inserted, finalUrl };
}