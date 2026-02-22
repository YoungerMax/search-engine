CREATE TABLE "feed" (
	"feed_url" text NOT NULL,
	"home_url" text,
	"name" text,
	"link" text,
	"last_published" timestamp,
	"last_fetched" timestamp,
	"image" text,
	CONSTRAINT "feed_feed_url_pk" PRIMARY KEY("feed_url")
);
--> statement-breakpoint
CREATE TABLE "item" (
	"url" text NOT NULL,
	"title" text,
	"description" text,
	"image" text,
	"content" text,
	"published" timestamp,
	"author" text,
	"feed_url" text NOT NULL,
	CONSTRAINT "item_url_pk" PRIMARY KEY("url")
);
--> statement-breakpoint
ALTER TABLE "item" ADD CONSTRAINT "item_feed_url_feed_feed_url_fk" FOREIGN KEY ("feed_url") REFERENCES "public"."feed"("feed_url") ON DELETE cascade ON UPDATE no action;