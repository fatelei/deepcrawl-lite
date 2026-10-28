import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

from deepcrawl import Crawler, DeepSeekFetcher, SQLiteStore
from deepcrawl.client import APIError, normalize_url


class CrawlTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "crawl.db"
        self.store = SQLiteStore(self.path)
        self.addCleanup(self.store.__exit__, None, None, None)

    def client(self, handler, **kwargs):
        return DeepSeekFetcher("test-key", transport=httpx.MockTransport(handler),
                               fetch_qps=100000, search_qps=100000, **kwargs)

    async def test_url_dedup_resume_and_content_dedup(self):
        calls = []

        def handle(request):
            calls.append(json.loads(request.content)["url"])
            self.assertEqual(request.headers["Authorization"], "Bearer test-key")
            self.assertEqual(request.url.path, "/v1/tools/fetch")
            return httpx.Response(200, json={"title": "标题", "content": "相同正文"})

        async with self.client(handle) as client:
            crawler = Crawler(client, self.store)
            await crawler.run(["https://example.com/a#one", "https://example.com/a#two", "https://example.com/b"])
            await crawler.run(["https://example.com/a"])
        self.assertEqual(len(calls), 2)
        self.assertEqual(self.store.stats()["pages"], 2)
        self.assertEqual(self.store.stats()["unique_contents"], 1)
        output = Path(self.tmp.name) / "out.jsonl"
        self.assertEqual(self.store.export(output), 2)
        records = [json.loads(line) for line in output.read_text().splitlines()]
        self.assertEqual(records[0]["content"], "相同正文")
        self.assertEqual(self.store.export(output, unique=True), 1)

    async def test_search_reuses_content_and_fetches_missing_body(self):
        calls = []

        def handle(request):
            calls.append(request.url.path)
            if request.url.path.endswith("search"):
                self.assertTrue(json.loads(request.content)["need_content"])
                return httpx.Response(200, json={"results": [
                    {"url": "https://example.com/a", "title": "A", "content": "search body"},
                    {"url": "https://example.com/b", "content": ""},
                ]})
            return httpx.Response(200, json={"title": "B", "content": "fetch body"})

        async with self.client(handle) as client:
            await Crawler(client, self.store).run(query="example")
        self.assertEqual(calls, ["/v1/tools/search", "/v1/tools/fetch"])
        self.assertEqual(self.store.stats()["done"], 2)

    async def test_transient_retries_and_retry_after(self):
        responses = [httpx.Response(429, headers={"Retry-After": "3"}),
                     httpx.Response(503), httpx.Response(200, json={"content": "ok"})]
        async with self.client(lambda request: responses.pop(0)) as client:
            with patch("deepcrawl.client.asyncio.sleep", new_callable=AsyncMock) as sleep:
                result = await client.fetch("https://example.com")
        self.assertEqual(result["content"], "ok")
        self.assertTrue(any(call.args[0] >= 3 for call in sleep.call_args_list))
        self.assertFalse(responses)

    async def test_permanent_failure_requires_explicit_retry(self):
        calls = []

        def handle(request):
            calls.append(1)
            return httpx.Response(404)

        async with self.client(handle) as client:
            crawler = Crawler(client, self.store)
            await crawler.run(["https://example.com"])
            await crawler.run()
            self.assertEqual(len(calls), 1)
            await crawler.run(retry_failed=True)
            self.assertEqual(len(calls), 2)
        self.assertEqual(self.store.stats()["failed"], 1)

    async def test_exhausted_transient_failure_resumes(self):
        responses = [httpx.Response(500), httpx.Response(200, json={"content": "ok"})]
        async with self.client(lambda request: responses.pop(0), retries=0) as client:
            crawler = Crawler(client, self.store)
            await crawler.run(["https://example.com"])
            self.assertEqual(self.store.stats()["failed"], 1)
            await crawler.run()
        self.assertEqual(self.store.stats()["done"], 1)

    async def test_account_errors_abort_batch(self):
        calls = []

        def handle(request):
            calls.append(1)
            return httpx.Response(402)

        async with self.client(handle) as client:
            with self.assertRaises(APIError):
                await Crawler(client, self.store, concurrency=1).run([
                    "https://example.com/a", "https://example.com/b"])
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.store.stats()["pending"], 1)

    async def test_refresh_detects_change_and_preserves_last_good_page_on_failure(self):
        responses = [httpx.Response(200, json={"content": "v1"}),
                     httpx.Response(200, json={"content": "v2"}), httpx.Response(404)]
        async with self.client(lambda request: responses.pop(0)) as client:
            crawler = Crawler(client, self.store)
            await crawler.run(["https://example.com"])
            await crawler.run(["https://example.com"], refresh=True)
            row = self.store.db.execute("SELECT * FROM pages").fetchone()
            self.assertEqual(row["changed"], 1)
            self.assertNotEqual(row["previous_hash"], row["content_hash"])
            await crawler.run(["https://example.com"], refresh=True)
        out = Path(self.tmp.name) / "out.jsonl"
        self.store.export(out)
        record = json.loads(out.read_text())
        self.assertEqual(record["content"], "v2")
        self.assertEqual(record["task_status"], "failed")

    async def test_cancelled_task_resumes_after_reopening_database(self):
        started = asyncio.Event()

        async def blocked(request):
            started.set()
            await asyncio.Event().wait()

        async with self.client(blocked) as client:
            task = asyncio.create_task(Crawler(client, self.store).run(["https://example.com"]))
            await started.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(self.store.stats()["running"], 1)
        with SQLiteStore(self.path) as reopened:
            async with self.client(lambda req: httpx.Response(200, json={"content": "recovered"})) as client:
                await Crawler(client, reopened).run()
            self.assertEqual(reopened.stats()["done"], 1)

    async def test_concurrency_bound(self):
        active = peak = 0

        async def handle(request):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.01)
            active -= 1
            return httpx.Response(200, json={"content": "ok"})

        async with self.client(handle) as client:
            await Crawler(client, self.store, concurrency=2).run(
                [f"https://example.com/{i}" for i in range(8)])
        self.assertEqual(peak, 2)

    async def test_empty_search_and_empty_fetch(self):
        async with self.client(lambda req: httpx.Response(200, json={"results": []})) as client:
            await Crawler(client, self.store).run(query="missing")
        self.assertEqual(self.store.stats()["pages"], 0)
        async with self.client(lambda req: httpx.Response(200, json={"content": " "})) as client:
            await Crawler(client, self.store).run(["https://example.com"])
        self.assertEqual(self.store.stats()["failed"], 1)

    async def test_network_timeout_retry(self):
        attempts = 0

        def handle(request):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise httpx.ReadTimeout("timeout")
            return httpx.Response(200, json={"content": "ok"})

        async with self.client(handle) as client:
            with patch("deepcrawl.client.asyncio.sleep", new_callable=AsyncMock):
                await client.fetch("https://example.com")
        self.assertEqual(attempts, 2)

    def test_process_lock_and_release(self):
        self.store.acquire()
        with SQLiteStore(self.path) as other:
            with self.assertRaises(RuntimeError):
                other.acquire()
            self.store.release()
            other.acquire()

    def test_export_cannot_overwrite_database(self):
        with self.assertRaises(ValueError):
            self.store.export(self.path)
        self.assertEqual(self.store.stats()["pages"], 0)

    def test_url_validation_and_atomic_enqueue(self):
        self.assertEqual(normalize_url(" HTTPS://EXAMPLE.COM/a?q=1#section "), "https://example.com/a?q=1")
        for invalid in ("file:///tmp/a", "https://user:pass@example.com", "https://example.com:abc"):
            with self.assertRaises(ValueError):
                self.store.enqueue(["https://example.com/good", invalid])
        self.assertEqual(self.store.stats()["pending"], 0)


if __name__ == "__main__":
    unittest.main()
