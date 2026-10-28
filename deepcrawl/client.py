from __future__ import annotations

import asyncio
import math
import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit, urlunsplit

import httpx


def normalize_url(url: str) -> str:
    """Remove fragments, preserving query order and path semantics."""
    url = url.strip()
    if len(url) > 8192 or any(ord(c) < 32 for c in url):
        raise ValueError("Invalid URL length or control character")
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ValueError("URL must be an absolute HTTP(S) URL")
    if parts.username is not None or parts.password is not None:
        raise ValueError("Credentials in URLs are not supported")
    _ = parts.port  # Validate the port.
    return urlunsplit((parts.scheme, parts.netloc.lower(), parts.path or "/", parts.query, ""))


class APIError(Exception):
    def __init__(self, message: str, *, retryable: bool = False, fatal: bool = False):
        super().__init__(message)
        self.retryable = retryable
        self.fatal = fatal


class RateLimiter:
    def __init__(self, qps: float):
        if not math.isfinite(qps) or qps <= 0:
            raise ValueError("qps must be finite and positive")
        self.interval = 1 / qps
        self.next_at = 0.0
        self.lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self.lock:
            await asyncio.sleep(max(0, self.next_at - time.monotonic()))
            self.next_at = time.monotonic() + self.interval


class DeepSeekFetcher:
    """One pooled client; independent Search/Fetch pacing, including retries."""

    def __init__(self, api_key: str, *, timeout: float = 60, retries: int = 3,
                 fetch_qps: float = 2, search_qps: float = 2,
                 transport: httpx.AsyncBaseTransport | None = None):
        if not api_key.strip():
            raise ValueError("Set DEEPSEEK_API_KEY first")
        if retries < 0 or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("Invalid retries or timeout")
        self.retries = retries
        self.limiters = {"fetch": RateLimiter(fetch_qps), "search": RateLimiter(search_qps)}
        self.http = httpx.AsyncClient(
            base_url="https://api.deepseek.com", timeout=timeout, transport=transport,
            headers={"Authorization": f"Bearer {api_key}"}, follow_redirects=False,
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.http.aclose()

    @staticmethod
    def retry_after(response: httpx.Response) -> float:
        value = response.headers.get("Retry-After", "0")
        try:
            delay = float(value)
        except ValueError:
            try:
                delay = (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds()
            except (ValueError, TypeError, OverflowError):
                return 0
        return max(0, delay) if math.isfinite(delay) else 0

    async def _post(self, tool: str, payload: dict) -> dict:
        for attempt in range(self.retries + 1):
            await self.limiters[tool].wait()
            delay = 0.0
            try:
                response = await self.http.post(f"/v1/tools/{tool}", json=payload)
            except httpx.TransportError:
                error = APIError("DeepSeek network/timeout error", retryable=True)
            else:
                status = response.status_code
                if status == 200:
                    try:
                        data = response.json()
                    except ValueError as exc:
                        raise APIError("DeepSeek returned invalid JSON") from exc
                    if not isinstance(data, dict):
                        raise APIError("DeepSeek returned an invalid response object")
                    return data
                error = APIError(
                    f"DeepSeek {tool} HTTP {status}",
                    retryable=status in (408, 429) or 500 <= status < 600,
                    fatal=status in (401, 402),
                )
                delay = self.retry_after(response)
            if not error.retryable or attempt == self.retries:
                raise error
            await asyncio.sleep(max(delay, min(30, 2 ** attempt) + random.uniform(0, 0.5)))
        raise AssertionError("unreachable")

    async def fetch(self, url: str) -> dict:
        data = await self._post("fetch", {"url": normalize_url(url)})
        if not isinstance(data.get("content"), str) or not data["content"].strip():
            raise APIError("Fetch returned no usable content")
        return {"title": data.get("title") if isinstance(data.get("title"), str) else "",
                "content": data["content"]}

    async def search(self, query: str, *, count: int = 10) -> list[dict]:
        if not query.strip() or len(query) > 128 or not 1 <= count <= 50:
            raise ValueError("Search requires a 1–128 character query and count 1–50")
        data = await self._post("search", {"query": query, "count": count, "need_content": True})
        results = data.get("results")
        if not isinstance(results, list):
            raise APIError("Search returned an invalid results list")
        for row in results:
            if not isinstance(row, dict) or not isinstance(row.get("url"), str):
                raise APIError("Search returned an invalid result")
            try:
                row["url"] = normalize_url(row["url"])
            except ValueError as exc:
                raise APIError("Search returned an invalid URL") from exc
            for key in ("title", "content"):
                if not isinstance(row.get(key), str):
                    row[key] = ""
        return results
