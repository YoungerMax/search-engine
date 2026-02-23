import asyncio
import logging
import os
import time

from dotenv import load_dotenv

from app.batch.bm25_stats import run as run_bm25
from app.batch.duplicate_detection import run as run_duplicates
from app.batch.link_graph_builder import run as run_link_graph
from app.batch.news_fetcher import run as run_news_fetcher
from app.batch.pagerank import run as run_pagerank
from app.batch.spellcheck_dictionary import run as run_spellcheck

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _batch_node_config() -> tuple[int, int]:
    total_nodes = max(1, int(os.environ.get("BATCH_TOTAL_NODES", "1")))
    node_index = int(os.environ.get("BATCH_NODE_INDEX", "0"))
    return total_nodes, node_index


def _should_run_global_jobs(total_nodes: int, node_index: int) -> bool:
    role = os.environ.get("BATCH_ROLE", "auto").strip().lower()
    if role == "coordinator":
        return True
    if role == "worker":
        return False
    return total_nodes == 1 or node_index == 0


async def run_once() -> None:
    total_nodes, node_index = _batch_node_config()
    run_global = _should_run_global_jobs(total_nodes, node_index)

    await run_news_fetcher()
    await run_duplicates()

    if not run_global:
        logger.info(
            "skipping global jobs on worker node_index=%s total_nodes=%s",
            node_index,
            total_nodes,
        )
        return

    await run_link_graph()
    await asyncio.gather(run_pagerank(), run_bm25(), run_spellcheck())


async def main() -> None:
    interval_s = int(os.environ["BATCH_INTERVAL_S"])
    logger.info("starting batch runner with interval=%ss", interval_s)

    while True:
        started = time.time()
        try:
            await run_once()
            elapsed = time.time() - started
            sleep_for = max(1, interval_s - int(elapsed))
            logger.info("batch cycle complete in %.2fs; sleeping %ss", elapsed, sleep_for)
            await asyncio.sleep(sleep_for)
        except Exception:
            logger.exception("batch cycle failed; retrying in 15s")
            await asyncio.sleep(15)


if __name__ == "__main__":
    asyncio.run(main())
