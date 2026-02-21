import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    user_agent: str = os.environ["CRAWLER_USER_AGENT"]
    queue_batch_size: int = int(os.environ["QUEUE_BATCH_SIZE"])
    crawler_concurrency: int = int(os.getenv("CRAWLER_CONCURRENCY", "8"))
    request_timeout_s: int = int(os.environ["REQUEST_TIMEOUT_S"])


settings = Settings()
