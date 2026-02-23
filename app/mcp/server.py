from __future__ import annotations

from app.api.search_service import perform_news_search, perform_web_search

try:
    from fastmcp import FastMCP
except ImportError as exc:  # pragma: no cover - runtime dependency guard
    raise RuntimeError(
        "FastMCP is required to run the MCP server. Install dependencies from requirements.txt."
    ) from exc


SERVER_TITLE = "OpenGoogle"
SERVER_DESCRIPTION = (
    "OpenGoogle MCP server for searching indexed web pages and news articles with separate tools."
)
SERVER_INSTRUCTIONS = (
    "Use search_web for general web pages and search_news for news content. "
    "Set limit and offset for pagination."
)

mcp = FastMCP(SERVER_TITLE)

# Populate optional metadata fields when supported by the installed FastMCP version.
if hasattr(mcp, "title"):
    mcp.title = SERVER_TITLE
if hasattr(mcp, "description"):
    mcp.description = SERVER_DESCRIPTION
if hasattr(mcp, "instructions"):
    mcp.instructions = SERVER_INSTRUCTIONS


def _bounded(limit: int, offset: int) -> tuple[int, int]:
    return max(1, min(limit, 100)), max(0, offset)


@mcp.tool(name="search_web", description="Search indexed web documents only.")
def search_web(query: str, limit: int = 20, offset: int = 0) -> dict[str, object]:
    """Run a search query against the web index."""
    bounded_limit, bounded_offset = _bounded(limit, offset)
    return perform_web_search(q=query, limit=bounded_limit, offset=bounded_offset)


@mcp.tool(name="search_news", description="Search indexed news articles only.")
def search_news(query: str, limit: int = 20, offset: int = 0) -> dict[str, object]:
    """Run a search query against the news index."""
    bounded_limit, bounded_offset = _bounded(limit, offset)
    return perform_news_search(q=query, limit=bounded_limit, offset=bounded_offset)


if __name__ == "__main__":
    mcp.run()
