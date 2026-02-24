from __future__ import annotations

import asyncio
import base64
import logging
import os
from html import unescape
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from app.common.db import get_conn_async
from app.crawler.normalization import normalize_url, registrable_domain
from app.crawler.tokenizer import tokenize

logger = logging.getLogger(__name__)

MAX_FEEDS_PER_RUN = 100
MAX_ITEMS_PER_FEED = 50


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _text(node, tag: str) -> str:
    el = node.find(tag)
    return (el.get_text(" ", strip=True) if el else "").strip()


def _clean_html_text(value: str) -> str:
    if not value:
        return ""
    decoded = unescape(value)
    return BeautifulSoup(decoded, "html.parser").get_text(" ", strip=True)


def _author_text(node) -> str:
    author = _text(node, "author")
    if author:
        return author

    for tag in ("dc:creator", "creator", "itunes:author", "dcterms:creator"):
        author = _text(node, tag)
        if author:
            return author

    return ""


def _image_url(node, base_url: str) -> str:
    image_url = ""

    media_content = node.find("media:content")
    if media_content:
        image_url = (media_content.get("url") or "").strip()

    if not image_url:
        media_thumbnail = node.find("media:thumbnail")
        if media_thumbnail:
            image_url = (media_thumbnail.get("url") or "").strip()

    if not image_url:
        enclosure = node.find("enclosure")
        if enclosure and "image" in (enclosure.get("type") or "").lower():
            image_url = (enclosure.get("url") or "").strip()

    if not image_url:
        image_url = (_text(node, "image") or _text(node, "thumbnail")).strip()

    if image_url:
        try:
            return normalize_url(urljoin(base_url, image_url))
        except Exception:
            return ""

    return ""


async def _fetch_image_base64(client: httpx.AsyncClient, image_url: str) -> str:
    if not image_url:
        return ""

    try:
        response = await client.get(image_url, headers={"User-Agent": "search-engine-news-fetcher/1.0"})
        if response.status_code >= 400 or not response.content:
            return ""
        return base64.b64encode(response.content).decode("ascii")
    except Exception:
        return ""


async def _parse_item_with_image(client: httpx.AsyncClient, feed_url: str, node) -> dict[str, object] | None:
    link = ""
    link_node = node.find("link")
    if link_node:
        if link_node.get("href"):
            link = link_node.get("href")
        else:
            link = link_node.get_text(strip=True)
    if not link:
        return None

    try:
        url = normalize_url(urljoin(feed_url, link))
    except Exception:
        return None

    title = _text(node, "title")
    description = _clean_html_text(_text(node, "description") or _text(node, "summary"))
    content = _text(node, "content") or _text(node, "content:encoded")
    author = _author_text(node)
    image = await _fetch_image_base64(client, _image_url(node, feed_url))

    published = (
        _parse_datetime(_text(node, "pubDate"))
        or _parse_datetime(_text(node, "published"))
        or _parse_datetime(_text(node, "updated"))
    )

    return {
        "url": url,
        "title": title,
        "description": description,
        "content": content,
        "author": author,
        "published_at": published,
        "image": image,
    }


async def _parse_feed_with_image(client: httpx.AsyncClient, feed_url: str, xml_text: str) -> tuple[dict[str, object], list[dict[str, object]]]:
    soup = BeautifulSoup(xml_text, "xml")

    channel = soup.find("channel")
    feed = soup.find("feed")
    source = channel or feed or soup

    title = _text(source, "title")

    link = ""
    link_node = source.find("link")
    if link_node:
        link = (link_node.get("href") or link_node.get_text(strip=True) or "").strip()
    if link:
        try:
            link = normalize_url(urljoin(feed_url, link))
        except Exception:
            link = ""

    last_published = (
        _parse_datetime(_text(source, "lastBuildDate"))
        or _parse_datetime(_text(source, "pubDate"))
        or _parse_datetime(_text(source, "updated"))
    )

    metadata: dict[str, object] = {
        "name": title,
        "link": link,
        "image": await _fetch_image_base64(client, _image_url(source, feed_url)),
        "last_published": last_published,
    }

    items: list[dict[str, object]] = []
    for node in soup.find_all(["item", "entry"]):
        parsed_item = await _parse_item_with_image(client, feed_url, node)
        if parsed_item is None:
            continue
        items.append(parsed_item)
        if len(items) >= MAX_ITEMS_PER_FEED:
            break

    item_dates = [item["published_at"] for item in items if isinstance(item.get("published_at"), datetime)]
    if item_dates:
        max_item_date = max(item_dates)
        current_last_published = metadata.get("last_published")
        if not isinstance(current_last_published, datetime) or max_item_date > current_last_published:
            metadata["last_published"] = max_item_date

    return metadata, items


async def run() -> None:
    total_nodes = max(1, int(os.environ.get("BATCH_TOTAL_NODES", "1")))
    node_index = int(os.environ.get("BATCH_NODE_INDEX", "0"))

    async with get_conn_async() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT feed_url
                FROM news_feeds
                WHERE COALESCE(next_fetch_at, now() - interval '1 second') <= now()
                  AND mod(abs(hashtext(feed_url)), %s) = %s
                ORDER BY next_fetch_at NULLS FIRST, last_fetched NULLS FIRST
                LIMIT %s
                """,
                (total_nodes, node_index, MAX_FEEDS_PER_RUN),
            )
            feeds = [row[0] for row in await cur.fetchall()]

    if not feeds:
        return

    timeout = httpx.Timeout(12.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        for feed_url in feeds:
            try:
                response = await client.get(feed_url, headers={"User-Agent": "search-engine-news-fetcher/1.0"})
                if response.status_code >= 400:
                    raise RuntimeError(f"status={response.status_code}")

                metadata, items = await _parse_feed_with_image(client, feed_url, response.text)
                await _persist_feed(feed_url, items, metadata)
            except Exception:
                logger.exception("failed processing feed=%s", feed_url)


async def _persist_feed(feed_url: str, items: list[dict[str, object]], metadata: dict[str, object]) -> None:
    now = datetime.now(timezone.utc)
    next_fetch_at = now + timedelta(minutes=20)

    async with get_conn_async() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE news_feeds
                SET last_fetched = %s,
                    next_fetch_at = %s,
                    name = COALESCE(NULLIF(%s, ''), name),
                    link = COALESCE(NULLIF(%s, ''), link),
                    image = COALESCE(NULLIF(%s, ''), image),
                    last_published = COALESCE(%s, last_published)
                WHERE feed_url = %s
                """,
                (
                    now,
                    next_fetch_at,
                    metadata.get("name", ""),
                    metadata.get("link", ""),
                    metadata.get("image", ""),
                    metadata.get("last_published"),
                    feed_url,
                ),
            )

            discovered_urls: set[str] = set()

            for item in items:
                await cur.execute(
                    """
                    INSERT INTO news_articles(url, feed_url, title, description, image, content, author, published_at, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,now())
                    ON CONFLICT (url) DO UPDATE SET
                      title = COALESCE(NULLIF(EXCLUDED.title, ''), news_articles.title),
                      description = COALESCE(NULLIF(EXCLUDED.description, ''), news_articles.description),
                      image = COALESCE(NULLIF(EXCLUDED.image, ''), news_articles.image),
                      content = CASE
                          WHEN COALESCE(news_articles.content, '') = '' THEN EXCLUDED.content
                          WHEN COALESCE(EXCLUDED.content, '') = '' THEN news_articles.content
                          ELSE EXCLUDED.content
                      END,
                      author = COALESCE(NULLIF(EXCLUDED.author, ''), news_articles.author),
                      published_at = COALESCE(EXCLUDED.published_at, news_articles.published_at),
                      updated_at = now()
                    """,
                    (
                        item["url"],
                        feed_url,
                        item["title"],
                        item["description"],
                        item["image"],
                        item["content"],
                        item["author"],
                        item["published_at"],
                    ),
                )

                discovered_urls.add(str(item["url"]))

                text = f"{item['title'] or ''} {item['description'] or ''} {item['content'] or ''}"
                terms = tokenize(text)
                if not terms:
                    continue

                await cur.execute(
                    "DELETE FROM tokens WHERE source_type = 2 AND article_url = %s",
                    (item["url"],),
                )
                if terms:
                    await cur.executemany(
                        "INSERT INTO tokens(doc_id, article_url, source_type, term, field, frequency, positions) VALUES (NULL,%s,2,%s,4,%s,'{}')",
                        ((item["url"], term, freq) for term, freq in terms.items()),
                    )

            if discovered_urls:
                await cur.executemany(
                    """
                    INSERT INTO crawl_queue(url, status, domain, attempt_count)
                    VALUES (%s, 'queued', %s, 0)
                    ON CONFLICT(url) DO NOTHING
                    """,
                    ((url, registrable_domain(url)) for url in discovered_urls),
                )


if __name__ == "__main__":
    asyncio.run(run())
