import os
import re
import asyncio
import aiohttp
from aiohttp import ClientTimeout, TCPConnector
from aiohttp_retry import RetryClient, ExponentialRetry
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from typing import Coroutine, Any

BASE_URL = "https://www.afro.who.int/health-topics/disease-outbreaks/outbreaks-and-other-emergencies-updates"
DOWNLOAD_DIR = "who_afro_bulletins"
PAGE_LIMIT = 10
CONCURRENT_DOWNLOADS = 20

os.makedirs(DOWNLOAD_DIR, exist_ok=True)


def sanitize(name: str) -> str:
    return re.sub(r"[^\w\-_. ]", "_", name) + ".pdf"


async def fetch_page_links(
    retry_client: RetryClient, page: int
) -> list[tuple[str, str]]:
    url = f"{BASE_URL}?page={page}" if page else BASE_URL
    async with retry_client.get(url) as resp:
        text = await resp.text()
    soup = BeautifulSoup(text, "html.parser")
    return [
        (
            str(a.get_text(strip=True) or os.path.basename(str(a["href"]))),
            urljoin(BASE_URL, str(a["href"])),
        )
        for a in soup.select("a[href$='.pdf']")
    ]


async def download_pdf(
    sem: asyncio.Semaphore, retry_client: RetryClient, label: str, url: str
):
    fname = sanitize(label)
    path = os.path.join(DOWNLOAD_DIR, fname)
    if os.path.exists(path):
        print(f"[SKIP] {fname}")
        return
    async with sem:
        print(f"[DOWNLOAD] {fname}")
        async with retry_client.get(url) as resp:
            with open(path, "wb") as f:
                async for chunk in resp.content.iter_chunked(8192):
                    f.write(chunk)


async def main():
    timeout = ClientTimeout(total=None)
    connector = TCPConnector(limit=CONCURRENT_DOWNLOADS)
    retry_options = ExponentialRetry(
        attempts=5, start_timeout=1, max_timeout=60, statuses={429, 500, 502, 503, 504}
    )
    sem = asyncio.Semaphore(CONCURRENT_DOWNLOADS)

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        headers={"User-Agent": "DownloaderBot/1.0"},
    ) as session:
        retry_client = RetryClient(session, retry_options=retry_options)
        tasks: list[Coroutine[Any, Any, None]] = []  # typing for tasks
        for page in range(PAGE_LIMIT):
            links = await fetch_page_links(retry_client, page)
            if not links:
                break
            print(f"Page {page}: found {len(links)} PDFs")
            for label, url in links:
                tasks.append(download_pdf(sem, retry_client, label, url))
        await asyncio.gather(*tasks)
        print(f"Finished downloading {len(tasks)} PDFs.")


if __name__ == "__main__":
    asyncio.run(main())
