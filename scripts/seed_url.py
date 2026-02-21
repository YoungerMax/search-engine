#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.crawler.queue_manager import QueueManager
from app.crawler.normalization import normalize_url


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Connect to Postgres and enqueue a seed URL."
    )
    parser.add_argument("url", help="Seed URL to add to crawl_queue")
    args = parser.parse_args()

    normalized = normalize_url(args.url)
    QueueManager().enqueue_url(args.url)
    print(f"Seed URL queued: {normalized}")


if __name__ == "__main__":
    main()
