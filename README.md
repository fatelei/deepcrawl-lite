# Deepcrawl Lite

A lightweight Python content collection framework powered by DeepSeek Search and Fetch. Collect public articles, gather research material, and prepare Markdown documents for a knowledge base.

Requires Python 3.11+. HTTPX is the only runtime dependency.

## Features

- Collect URLs directly or discover pages through search.
- Reuse search results that contain text; fetch pages when content is missing.
- Run concurrent workers with independent Search and Fetch rate limits.
- Retry transient failures with exponential backoff, jitter, and `Retry-After` support.
- Persist tasks and documents in SQLite and resume interrupted runs.
- Deduplicate URLs and exact content, and detect changes on refresh.
- Export documents as JSONL.
- Use either the CLI or the Python API.

## Installation

From the project directory:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
export DEEPSEEK_API_KEY='your-api-key'
deepcrawl --help
```

On Windows, activate the environment with `.venv\Scripts\activate` and set the environment variable using your shell. Deepcrawl does not automatically load `.env` files or persist your API key.

## Command-line usage

```bash
# Collect a URL.
deepcrawl crawl https://example.com/article --db articles.db

# Read one URL per line; blank lines and lines starting with # are ignored.
deepcrawl crawl --file urls.txt --db articles.db --concurrency 5 --qps 2

# Search for pages, reusing available content before falling back to Fetch.
deepcrawl search 'Python site:docs.python.org' --count 10 --db articles.db

# Resume pending, interrupted, and retryable failed tasks.
deepcrawl resume --db articles.db

# Also retry permanent failures, such as 403/404, after addressing their cause.
deepcrawl resume --db articles.db --retry-failed

# Fetch a URL again and compare its content hash.
deepcrawl crawl https://example.com/article --db articles.db --refresh

# Inspect and export local data without an API key.
deepcrawl status --db articles.db
deepcrawl export --db articles.db --output articles.jsonl
deepcrawl export --db articles.db --output unique.jsonl --unique
```

You can also use `python -m deepcrawl` instead of `deepcrawl`.

## Python API

```python
import asyncio
import os

from deepcrawl import Crawler, DeepSeekFetcher, SQLiteStore


async def main():
    with SQLiteStore("articles.db") as store:
        async with DeepSeekFetcher(os.environ["DEEPSEEK_API_KEY"]) as fetcher:
            crawler = Crawler(fetcher, store, concurrency=5)
            print(await crawler.run(["https://example.com/article"]))
            print(await crawler.run(query="Python site:docs.python.org", count=10))
        store.export("articles.jsonl")


asyncio.run(main())
```

A replacement backend can provide asynchronous `fetch(url)` and `search(query, count=...)` methods with the same return fields as `DeepSeekFetcher`. Expected backend failures should raise `deepcrawl.client.APIError`. Version 0.1 accepts URL iterables and a `query` argument directly; it does not introduce separate source classes.

## Task lifecycle and retries

Each `run()` processes its inputs together with resumable tasks already in the database. Successfully collected URLs are skipped by default. `refresh=True` requeues only the URLs supplied or discovered in that call.

The defaults are five workers and approximately two requests per second for each endpoint. Concurrency and request rate are separate controls; rate limits also apply to retries. Processes using different databases do not share a rate limiter, so configure their combined traffic accordingly.

HTTP 408, 429, 5xx responses and network/timeout failures use exponential backoff with jitter. `Retry-After` is honored. Each request gets up to three additional attempts by default. HTTP 401 and 402 stop the batch; fix the key or account balance and run `resume`. Other permanent failures are recorded while collection continues, and require explicit retry.

Committed data survives interruption. The next run recovers tasks left in the `running` state. A crash after a remote request succeeds but before its local commit may repeat the request: execution is at least once, not exactly once.

Only one collector may use a database at a time. A transaction in a separate SQLite `.lock` file provides a process lock that is released when the process exits. Do not delete that file while a collector is running. Keep the database on a local disk.

Collection commands return these exit codes:

| Code | Meaning |
| --- | --- |
| `0` | No failed tasks remain in the database. |
| `1` | Failed tasks remain, including historical permanent failures. |
| `2` | Configuration, account, or runtime error. |
| `130` | Interrupted by the user. |

Failure details are stored in `tasks.error` and can be inspected with SQLite.

## Storage and export

The SQLite database contains three tables:

| Table | Contents |
| --- | --- |
| `tasks` | Task state, task attempt count, queued search content, and errors. |
| `pages` | Metadata for the most recent successful collection of each URL. |
| `contents` | Markdown stored by SHA-256 hash; identical content is stored once. |

JSONL records include `url`, `title`, `content`, `content_hash`, `previous_hash`, `changed`, `fetched_at`, `source`, `task_status`, and `last_error`. Timestamps use UTC ISO 8601. The source is either `search` or `fetch`; `fetched_at` records local acquisition time, not publication time or search index freshness.

`changed` compares the two most recent successful collections and is false for the first collection. A failed refresh preserves the previous successful content and exports the failed task status alongside it.

By default, exports preserve a record for every URL. With `--unique`, only the first URL in lexical order for each content hash is exported. Exporting overwrites the specified output file.

URL normalization removes fragments, normalizes scheme/host case, and fills an empty path. It preserves query parameters and their order. Content deduplication uses exact Markdown bytes, not semantic similarity. Historical bodies may remain in `contents`, but this version does not provide a complete revision history or garbage collection.

## Scope and limitations

- Search performs one query with up to 50 results. It does not guarantee complete site coverage or implement pagination or recursive discovery.
- Fetch provides titles and Markdown, not a DOM, CSS selectors, login sessions, or browser interactions.
- Search content may be incomplete. Deepcrawl only checks whether it is nonempty; use `crawl URL --refresh` to request a fresh Fetch result.
- Version 0.1 does not include scheduling, sitemap parsing, LLM field extraction, semantic deduplication, or distributed queues.
- The API key is sent only in request headers. Remote error response bodies are not logged. URLs are sent to DeepSeek; collected content is stored locally.
- The timeout applies to HTTP phases, not the entire collection job. Target-site policies and per-domain rate limits may still cause failures.

The integration follows the [DeepSeek Search & Fetch API documentation](https://api-docs.deepseek.com/guides/search/). Live calls are subject to account billing and quotas.

## Testing

```bash
python -m unittest discover -s tests -v
```

The tests use HTTPX MockTransport and require neither network access nor a real API key. They cover content reuse, Fetch fallback, URL/content deduplication, retries, account errors, concurrency limits, refreshes, cancellation recovery, process locking, exports, and CLI behavior.

Version 0.1.0 passed all 16 offline tests and installation/CLI checks on Python 3.14. Live DeepSeek API integration has not yet been verified. Set your API key and run one of the collection commands above to perform a live check.
