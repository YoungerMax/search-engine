import asyncio
from fastmcp import Client

async def main():
    # Replace the connection URI with your specific server's address
    # e.g., "http://localhost:8000" or a specific stdio command
    async with Client("http://localhost:8000/mcp") as client:
        # Call the list_tools() method to retrieve the tools offered by the server
        tools = await client.list_tools()

        print(f"Available tools ({len(tools)}):")
        # Iterate over the list of tools and print their names and descriptions
        for tool in tools:
            print(f"* **{tool.name}**: {tool.description}")
        
        result = await client.call_tool('search_news', { 'query': 'google' })
        print(result.content[0].text)

if __name__ == "__main__":
    # Run the asynchronous function
    asyncio.run(main())
