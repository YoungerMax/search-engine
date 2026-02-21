import logging
import os
import time

from dotenv import load_dotenv

from app.batch.bm25_stats import run as run_bm25
from app.batch.duplicate_detection import run as run_duplicates
from app.batch.link_graph_builder import run as run_link_graph
from app.batch.pagerank import run as run_pagerank

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_once() -> None:
    run_duplicates()
    run_link_graph()
    run_pagerank()
    run_bm25()


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
