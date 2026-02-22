import httpx
import asyncio

with open('feeds.txt') as fp:
    feeds_urls = fp.readlines()

client = httpx.AsyncClient(timeout=1000)

async def main():
    await asyncio.gather(*[ client.post('http://localhost:3000/feeds', params={'url': feed_url}) for feed_url in feeds_urls ])

asyncio.run(main())