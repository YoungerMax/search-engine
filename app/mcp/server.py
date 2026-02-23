from __future__ import annotations

import asyncio
import inspect

from app.api.search_service import perform_news_search, perform_web_search

try:
    from fastmcp import FastMCP
except ImportError as exc:  # pragma: no cover - runtime dependency guard
    raise RuntimeError(
        "FastMCP is required to run the MCP server. Install dependencies from requirements.txt."
    ) from exc


SERVER_TITLE = "OpenGoogle"
SERVER_INSTRUCTIONS = (
    "Use search_web for general web pages and search_news for news content. "
    "Set limit and offset for pagination."
)

mcp = FastMCP(
    name=SERVER_TITLE,
    instructions=SERVER_INSTRUCTIONS,
    version='1',

)

def _bounded(limit: int, offset: int) -> tuple[int, int]:
    return max(1, min(limit, 100)), max(0, offset)


@mcp.tool(name="search_web", description="Search websites and web documents.")
def search_web(query: str, limit: int = 20, offset: int = 0) -> dict[str, object]:
    """Run a search query against the web index."""
    bounded_limit, bounded_offset = _bounded(limit, offset)
    result = perform_web_search(q=query, limit=bounded_limit, offset=bounded_offset)
    if inspect.isawaitable(result):
        return asyncio.run(result)
    return result


@mcp.tool(name="search_news", description="Search news articles.")
def search_news(query: str, limit: int = 20, offset: int = 0) -> dict[str, object]:
    """Run a search query against the news index."""
    bounded_limit, bounded_offset = _bounded(limit, offset)
    result = perform_news_search(q=query, limit=bounded_limit, offset=bounded_offset)
    if inspect.isawaitable(result):
        return asyncio.run(result)
    return result


if __name__ == "__main__":
    mcp.run("http")
