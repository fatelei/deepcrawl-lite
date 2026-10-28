from __future__ import annotations

import asyncio
import json
from collections.abc import Iterable

from .client import APIError, DeepSeekFetcher
from .store import SQLiteStore


class Crawler:
    def __init__(self, fetcher: DeepSeekFetcher, store: SQLiteStore, *, concurrency: int = 5):
        if concurrency < 1:
            raise ValueError("concurrency must be positive")
        self.fetcher, self.store, self.concurrency = fetcher, store, concurrency

    async def run(self, urls: Iterable[str] = (), *, query: str | None = None,
                  count: int = 10, refresh: bool = False, retry_failed: bool = False) -> dict:
        """Process supplied inputs plus any resumable tasks already in the database."""
        self.store.acquire()
        workers = []
        try:
            self.store.enqueue(list(urls), refresh=refresh)
            if query is not None:
                results = await self.fetcher.search(query, count=count)
                seeds = {r["url"]: r for r in results if r["content"].strip()}
                self.store.enqueue([r["url"] for r in results], refresh=refresh, seeds=seeds)
            self.store.recover(retry_failed=retry_failed)

            async def worker():
                while row := self.store.claim():
                    url = row["url"]
                    try:
                        if row["seed"]:
                            page = json.loads(row["seed"])
                            source = "search"
                        else:
                            page = await self.fetcher.fetch(url)
                            source = "fetch"
                    except APIError as exc:
                        # Account errors stop the batch, but can resume after key/balance repair.
                        self.store.fail(url, str(exc), exc.retryable or exc.fatal)
                        if exc.fatal:
                            raise
                    else:
                        self.store.save(url, page["title"], page["content"], source)

            workers = [asyncio.create_task(worker()) for _ in range(self.concurrency)]
            await asyncio.gather(*workers)
            return self.store.stats()
        finally:
            for worker_task in workers:
                if not worker_task.done():
                    worker_task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            self.store.release()
