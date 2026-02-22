import { pgTable, text, timestamp, primaryKey, index, real } from "drizzle-orm/pg-core";
import { relations, sql } from "drizzle-orm";

export const feeds = pgTable("feed", {
    feedUrl: text("feed_url").notNull(),
    homeUrl: text("home_url"),
    name: text("name"),
    link: text("link"),
    lastPublished: timestamp("last_published"),
    lastFetched: timestamp("last_fetched"),
    image: text("image"),
    nextFetchAt: timestamp("next_fetch_at"),
    publishRatePerHour: real("publish_rate_per_hour"),
}, (table) => ({
    pk: primaryKey({ columns: [table.feedUrl] }),
}));

export const items = pgTable("item", {
    url: text("url").notNull(),
    title: text("title"),
    description: text("description"),
    image: text("image"),
    content: text("content"),
    published: timestamp("published"),
    author: text("author"),
    feedUrl: text("feed_url").notNull().references(() => feeds.feedUrl, { onDelete: "cascade" }),
}, (table) => ({
    pk: primaryKey({ columns: [table.url] }),
    searchIdx: index("item_search_idx").using("gin", sql`to_tsvector('english', coalesce(${table.title}, '') || ' ' || coalesce(${table.description}, '') || ' ' || coalesce(${table.content}, ''))`),
}));

export const feedsRelations = relations(feeds, ({ many }) => ({
    items: many(items),
}));

export const itemsRelations = relations(items, ({ one }) => ({
    feed: one(feeds, {
        fields: [items.feedUrl],
        references: [feeds.feedUrl],
    }),
}));
