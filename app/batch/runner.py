import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor

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


def run_once() -> None:
    total_nodes, node_index = _batch_node_config()
    run_global = _should_run_global_jobs(total_nodes, node_index)

    # Sharded tasks run on every node.
    run_news_fetcher()
    run_duplicates()

    if not run_global:
        logger.info(
            "skipping global jobs on worker node_index=%s total_nodes=%s",
            node_index,
            total_nodes,
        )
        return

    run_link_graph()

    # Expensive global tasks run concurrently to reduce wall-clock time.
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(run_pagerank),
            executor.submit(run_bm25),
            executor.submit(run_spellcheck),
        ]
        for future in futures:
            future.result()


def main() -> None:
    interval_s = int(os.environ["BATCH_INTERVAL_S"])
    logger.info("starting batch runner with interval=%ss", interval_s)

    while True:
        started = time.time()
        try:
            run_once()
            elapsed = time.time() - started
            sleep_for = max(1, interval_s - int(elapsed))
            logger.info("batch cycle complete in %.2fs; sleeping %ss", elapsed, sleep_for)
            time.sleep(sleep_for)
        except Exception:
            logger.exception("batch cycle failed; retrying in 15s")
            time.sleep(15)


if __name__ == "__main__":
    main()
