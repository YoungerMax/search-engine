# 🔎 Distributed Search Engine Crawler

Build a **production-style, horizontally scalable** crawler + indexing pipeline with Python, Postgres, batch jobs, and a FastAPI search API.

---

## ✨ Why this project?

- ⚡ **Fast stateless workers** for crawl throughput.
- 🧠 **Offline global computation** (PageRank, link resolution, BM25 stats).
- 🗄️ **Migration-first schema management** with Alembic.
- 🐳 **Docker Compose + Swarm friendly** deployment workflow.

---

## 🧱 Architecture at a glance

```text
Crawler Workers (append-heavy writes)
        ↓
      Postgres
        ↓
Batch Jobs (duplicate detection, link graph, pagerank, bm25)
        ↓
     Search API
```

---

## 🚀 Quick Start

### 1) Configure environment

This repo ships with a baseline `.env`. Update values as needed:

- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_DB`
- `CRAWLER_USER_AGENT`
- `QUEUE_BATCH_SIZE`
- `CRAWLER_CONCURRENCY`
- `REQUEST_TIMEOUT_S`
- `BATCH_INTERVAL_S`
- `BATCH_TOTAL_NODES` (optional, for distributed batch workers)
- `BATCH_NODE_INDEX` (optional, for distributed batch workers)
- `BATCH_ROLE` (`auto`, `coordinator`, `worker`)

### 2) Start the stack

```bash
docker compose up --build
```

### 3) Query search API

```bash
curl 'http://localhost:8000/search?q=example'
curl 'http://localhost:8000/search/web?q=example'
curl 'http://localhost:8000/search/news?q=example'
curl 'http://localhost:8000/search?q=example&limit=10&offset=0'
```

### 4) Run MCP search server

Expose search as MCP tools (`search_web`, `search_news`) for MCP-compatible clients:

```bash
python -m app.mcp.server
```

Server name: `OpenGoogle`.

### Seed a URL into the crawl queue

```bash
python scripts/seed_url.py 'https://example.com'
```

---

## 🔌 API

### `GET /search`

Alias of `/search/web` (web-only results).

Query parameters:

- `q` (required): search query text
- `limit` (optional, default 20, max 100)
- `offset` (optional, default 0)

### `GET /search/web`

Same query parameters as `/search`. Returns only web results:

```json
{
  "results": [
    {
      "title": "...",
      "description": "...",
      "url": "https://...",
      "score": 1.234
    }
  ],
  "count": 1
}
```

### `GET /search/news`

Same query parameters as `/search`. Returns only news results.

---

## 🧩 Components

- `app/crawler/queue_manager.py`: queue transitions + batched dequeue.
- `app/crawler/worker.py`: fetch → parse → validate → tokenize → persist.
- `app/batch/*.py`: offline global jobs, including integrated RSS/Atom news fetcher.
- `app/batch/runner.py`: always-running batch scheduler loop.
- `app/api/main.py`: FastAPI `/search`, `/search/web`, and `/search/news` endpoints.
- `app/api/search_service.py`: shared search execution + ranking logic used by API and MCP layers.
- `app/mcp/server.py`: FastMCP server exposing `search_web` and `search_news` tools.
- `alembic/`: versioned migrations (single schema source of truth).
- `scripts/update_cluster.sh`: Swarm update with migration window.

---

## 🐝 Swarm Deploy / Update

Builds from `main` publish a multi-arch image (`linux/amd64` and `linux/arm64`) to `ghcr.io/youngermax/search-engine:latest` via GitHub Actions.
Swarm deploys pull that image directly from GHCR on either architecture.

```bash
docker swarm init
./scripts/update_cluster.sh
```

Update flow:

1. Deploy stack definition.
2. Scale migrator + crawler/API/batch down.
3. Wait for Postgres to be running.
4. Run migrator with retries (handles Postgres startup windows).
5. Scale crawler/API/batch back up.

---

## ⏱️ Batch cadence

`batch-jobs` runs continuously and executes the full pipeline every `BATCH_INTERVAL_S` seconds.


## 📰 News integration

- News feeds and articles are stored in Postgres via **Alembic migrations** (`news_feeds`, `news_articles`) and the unified `tokens` table (source-aware rows for both web and news).
- Crawler workers auto-discover RSS/Atom `<link>` metadata and seed `news_feeds`.
- Batch jobs fetch feeds and index news terms for `/search/news` results.
- Alembic is the canonical migration engine for the integrated stack.


## ⚙️ Distributed batch mode

Run multiple `batch-jobs` nodes with `BATCH_TOTAL_NODES` and unique `BATCH_NODE_INDEX` values.
Sharded work (`duplicate_detection`, `news_fetcher`) runs on all nodes; global work (`link_graph`, `pagerank`, `bm25`, `spellcheck`) runs only on the coordinator (node 0 by default, or forced with `BATCH_ROLE=coordinator`).
