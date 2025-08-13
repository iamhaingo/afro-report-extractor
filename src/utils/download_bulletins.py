import os
import re
import asyncio
import aiohttp
from aiohttp import ClientTimeout, TCPConnector
from aiohttp_retry import RetryClient, ExponentialRetry
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from typing import Coroutine, Any

# Configuration
BASE_URL = "https://www.afro.who.int/health-topics/disease-outbreaks/outbreaks-and-other-emergencies-updates"
DOWNLOAD_DIR = "who_afro_bulletins"
PAGE_LIMIT = 10
CONCURRENT_DOWNLOADS = 5

# Ensure download directory exists
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


# Sanitize filename (optional, but useful for safety)
def sanitize(name: str) -> str:
    return re.sub(r"[^\w\-_.]", "_", name)


# Extract list of (filename, full_url) for PDFs on a page
async def fetch_page_links(
    retry_client: RetryClient, page: int
) -> list[tuple[str, str]]:
    url = f"{BASE_URL}?page={page}" if page else BASE_URL
    async with retry_client.get(url) as resp:
        html = await resp.text()
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.select("a[href$='.pdf']"):
        href = str(a["href"])
        full_url = urljoin(BASE_URL, href)
        filename = os.path.basename(urlparse(full_url).path)
        links.append((filename, full_url))
    return links


# Download a single PDF file
async def download_pdf(
    sem: asyncio.Semaphore, retry_client: RetryClient, filename: str, url: str
):
    sanitized_name = sanitize(filename)
    filepath = os.path.join(DOWNLOAD_DIR, sanitized_name)
    if os.path.exists(filepath):
        print(f"[SKIP] {sanitized_name}")
        return

    async with sem:
        print(f"[DOWNLOAD] {sanitized_name}")
        async with retry_client.get(url) as resp:
            if resp.status != 200:
                print(
                    f"[ERROR] Failed to download {sanitized_name} (Status: {resp.status})"
                )
                return
            with open(filepath, "wb") as f:
                async for chunk in resp.content.iter_chunked(8192):
                    f.write(chunk)


# Main asynchronous entry point
async def main():
    timeout = ClientTimeout(total=None)
    connector = TCPConnector(limit=CONCURRENT_DOWNLOADS)
    retry_options = ExponentialRetry(
        attempts=5,
        start_timeout=1,
        max_timeout=120,
        statuses={429, 500, 502, 503, 504},
    )
    semaphore = asyncio.Semaphore(CONCURRENT_DOWNLOADS)

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        headers={"User-Agent": "DownloaderBot/1.0"},
    ) as session:
        retry_client = RetryClient(session, retry_options=retry_options)
        tasks: list[Coroutine[Any, Any, None]] = []

        for page in range(PAGE_LIMIT):
            links = await fetch_page_links(retry_client, page)
            if not links:
                print(f"[INFO] No PDFs found on page {page}")
                break
            print(f"[INFO] Page {page}: found {len(links)} PDFs")
            for filename, url in links:
                tasks.append(download_pdf(semaphore, retry_client, filename, url))

        await asyncio.gather(*tasks)
        print(f"[DONE] Downloaded {len(tasks)} PDFs.")


# Run the script
if __name__ == "__main__":
    asyncio.run(main())
