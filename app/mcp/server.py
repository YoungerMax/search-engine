from __future__ import annotations

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
async def search_web(query: str, limit: int = 10, offset: int = 0) -> str:
    """Run a search query against the web index."""
    bounded_limit, bounded_offset = _bounded(limit, offset)
    results = await perform_web_search(q=query, limit=bounded_limit, offset=bounded_offset)

    llm_results = ""

    for result in results.results:
        llm_results += f"[{result.url}]({result.title})"
        llm_results += '\n'
        llm_results += result.description
        llm_results += '\n'
        llm_results += '\n'
    
    return llm_results.strip()


@mcp.tool(name="search_news", description="Search news articles.")
async def search_news(query: str, limit: int = 20, offset: int = 0) -> str:
    """Run a search query against the news index."""
    bounded_limit, bounded_offset = _bounded(limit, offset)
    results = await perform_news_search(q=query, limit=bounded_limit, offset=bounded_offset)

    llm_results = ""

    for result in results.results:
        llm_results += f"[{result.url}]({result.title})"
        llm_results += '\n'
        llm_results += result.description
        llm_results += '\n'
        llm_results += '\n'
    
    return llm_results.strip()



if __name__ == "__main__":
    mcp.run("http")