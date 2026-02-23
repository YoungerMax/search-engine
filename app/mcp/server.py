from __future__ import annotations

from app.api.main import perform_search

try:
    from fastmcp import FastMCP
except ImportError as exc:  # pragma: no cover - runtime dependency guard
    raise RuntimeError(
        "FastMCP is required to run the MCP server. Install dependencies from requirements.txt."
    ) from exc


mcp = FastMCP("search-engine")


@mcp.tool(name="web_search", description="Search indexed web and news documents.")
def web_search(query: str, limit: int = 20, offset: int = 0) -> dict[str, object]:
    """Run a search query against the local index and return ranked results."""
    bounded_limit = max(1, min(limit, 100))
    bounded_offset = max(0, offset)
    return perform_search(q=query, limit=bounded_limit, offset=bounded_offset)


if __name__ == "__main__":
    mcp.run()
