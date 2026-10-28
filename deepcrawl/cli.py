import argparse
import asyncio
import json
import os
import sqlite3
import sys
from pathlib import Path

from .client import APIError, DeepSeekFetcher
from .crawler import Crawler
from .store import SQLiteStore


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description="DeepSeek Search/Fetch content collector")
    commands = root.add_subparsers(dest="command", required=True)
    for name in ("crawl", "search", "resume", "status", "export"):
        command = commands.add_parser(name)
        command.add_argument("--db", default="deepcrawl.db")
        if name in ("crawl", "search", "resume"):
            command.add_argument("--concurrency", type=int, default=5)
            command.add_argument("--qps", type=float, default=2, help="Request rate per endpoint")
            command.add_argument("--timeout", type=float, default=60)
            command.add_argument("--retries", type=int, default=3, help="Extra attempts per request")
            command.add_argument("--retry-failed", action="store_true", help="Also retry permanent failures")
        if name in ("crawl", "search"):
            command.add_argument("--refresh", action="store_true", help="Refetch the supplied/discovered URLs")
        if name == "crawl":
            command.add_argument("urls", nargs="*")
            command.add_argument("--file", type=Path, help="UTF-8 URL list, one URL per line")
        if name == "search":
            command.add_argument("query")
            command.add_argument("--count", type=int, default=10)
        if name == "export":
            command.add_argument("--output", type=Path, required=True)
            command.add_argument("--unique", action="store_true", help="One record per exact content hash")
    return root


async def collect(args, store):
    urls = getattr(args, "urls", [])
    if getattr(args, "file", None):
        urls += [line.strip() for line in args.file.read_text(encoding="utf-8").splitlines()
                 if line.strip() and not line.lstrip().startswith("#")]
    if args.command == "crawl" and not urls:
        raise ValueError("Provide URLs or --file; use resume for pending tasks")
    if args.concurrency < 1:
        raise ValueError("concurrency must be positive")
    async with DeepSeekFetcher(
        os.environ.get("DEEPSEEK_API_KEY", ""), timeout=args.timeout, retries=args.retries,
        fetch_qps=args.qps, search_qps=args.qps,
    ) as fetcher:
        return await Crawler(fetcher, store, concurrency=args.concurrency).run(
            urls, query=getattr(args, "query", None), count=getattr(args, "count", 10),
            refresh=getattr(args, "refresh", False), retry_failed=args.retry_failed,
        )


def main() -> int:
    args = parser().parse_args()
    try:
        with SQLiteStore(args.db) as store:
            if args.command == "status":
                result = store.stats()
            elif args.command == "export":
                result = {"exported": store.export(args.output, unique=args.unique), "output": str(args.output)}
            else:
                result = asyncio.run(collect(args, store))
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return int(args.command in ("crawl", "search", "resume") and result["failed"] > 0)
    except KeyboardInterrupt:
        print("Interrupted. Run deepcrawl resume with the same --db to continue.", file=sys.stderr)
        return 130
    except (APIError, ValueError, RuntimeError, OSError, sqlite3.Error) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
