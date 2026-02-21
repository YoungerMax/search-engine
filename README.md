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
- `REQUEST_TIMEOUT_S`
- `BATCH_INTERVAL_S`

### 2) Start the stack

```bash
docker compose up --build
```

### 3) Query search API

```bash
curl 'http://localhost:8000/search?q=example'
curl 'http://localhost:8000/search?q=example&limit=10&offset=0'
```

---

## 🔌 API

### `GET /search`

Query parameters:

- `q` (required): search query text
- `limit` (optional, default 20, max 100)
- `offset` (optional, default 0)

Response shape:

```json
{
  "results": [
    {
      "title": "...",
      "description": "...",
      "url": "https://...",
      "score": 1.234
    }
  ]
}
```

---

## 🧩 Components

- `app/crawler/queue_manager.py`: queue transitions + batched dequeue.
- `app/crawler/worker.py`: fetch → parse → validate → tokenize → persist.
- `app/batch/*.py`: offline global jobs.
- `app/batch/runner.py`: always-running batch scheduler loop.
- `app/api/main.py`: FastAPI `/search` endpoint.
- `alembic/`: versioned migrations (single schema source of truth).
- `scripts/update_cluster.sh`: Swarm update with migration window.

---

## 🐝 Swarm Deploy / Update

Builds from `main` publish `ghcr.io/youngermax/search-engine:latest` via GitHub Actions.
Swarm deploys pull that image directly from GHCR.

```bash
docker swarm init
./scripts/update_cluster.sh
```

Update flow:

1. Deploy stack definition.
2. Scale crawler/API/batch down.
3. Force migrator and wait for completion.
4. Scale services back up.

---

## ⏱️ Batch cadence

`batch-jobs` runs continuously and executes the full pipeline every `BATCH_INTERVAL_S` seconds.
