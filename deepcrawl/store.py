from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .client import normalize_url


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


class SQLiteStore:
    """Durable task queue and content-addressed document storage."""

    def __init__(self, path: str | Path):
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS tasks (
                url TEXT PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                retryable INTEGER NOT NULL DEFAULT 0,
                error TEXT,
                seed TEXT,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS contents (
                hash TEXT PRIMARY KEY, content TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS pages (
                url TEXT PRIMARY KEY, title TEXT NOT NULL,
                content_hash TEXT NOT NULL, previous_hash TEXT,
                changed INTEGER NOT NULL, fetched_at TEXT NOT NULL,
                source TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS tasks_status ON tasks(status);
        """)
        self._lock = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.release()
        self.db.close()

    def acquire(self) -> None:
        # A separate SQLite write transaction acts as a crash-safe process lock.
        # It does not block progress commits to the data database.
        if self._lock is not None:
            raise RuntimeError("Store is already in use by a crawler")
        lock = sqlite3.connect(str(self.path) + ".lock", timeout=0)
        try:
            lock.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            lock.close()
            raise RuntimeError("Another crawler is using this database") from exc
        self._lock = lock

    def release(self) -> None:
        if self._lock is not None:
            self._lock.close()
            self._lock = None

    def enqueue(self, urls: list[str], *, refresh: bool = False,
                seeds: dict[str, dict] | None = None) -> None:
        normalized = [normalize_url(url) for url in urls]
        with self.db:
            for url in normalized:
                seed = json.dumps(seeds[url], ensure_ascii=False) if seeds and url in seeds else None
                self.db.execute(
                    "INSERT OR IGNORE INTO tasks(url, seed, updated_at) VALUES (?, ?, ?)",
                    (url, seed, now()),
                )
                if refresh:
                    self.db.execute(
                        "UPDATE tasks SET status='pending', seed=?, error=NULL, retryable=0, updated_at=? WHERE url=?",
                        (seed, now(), url),
                    )
                elif seed:
                    self.db.execute("UPDATE tasks SET seed=? WHERE url=? AND status='pending'", (seed, url))

    def recover(self, *, retry_failed: bool = False) -> None:
        with self.db:
            self.db.execute("""
                UPDATE tasks SET status='pending', updated_at=?
                WHERE status='running' OR (status='failed' AND (retryable=1 OR ?))
            """, (now(), int(retry_failed)))

    def claim(self):
        with self.db:
            row = self.db.execute("SELECT url, seed FROM tasks WHERE status='pending' ORDER BY rowid LIMIT 1").fetchone()
            if row:
                self.db.execute("UPDATE tasks SET status='running', attempts=attempts+1, updated_at=? WHERE url=?", (now(), row["url"]))
        return row

    def save(self, url: str, title: str, content: str, source: str) -> None:
        digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
        with self.db:
            old = self.db.execute("SELECT content_hash FROM pages WHERE url=?", (url,)).fetchone()
            previous = old[0] if old else None
            self.db.execute("INSERT OR IGNORE INTO contents VALUES (?, ?)", (digest, content))
            self.db.execute("""
                INSERT OR REPLACE INTO pages VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (url, title, digest, previous, int(previous is not None and previous != digest), now(), source))
            self.db.execute("UPDATE tasks SET status='done', error=NULL, retryable=0, seed=NULL, updated_at=? WHERE url=?", (now(), url))

    def fail(self, url: str, error: str, retryable: bool) -> None:
        with self.db:
            self.db.execute("UPDATE tasks SET status='failed', error=?, retryable=?, updated_at=? WHERE url=?", (error, int(retryable), now(), url))

    def stats(self) -> dict:
        states = {key: 0 for key in ("pending", "running", "done", "failed")}
        states.update(dict(self.db.execute("SELECT status, count(*) FROM tasks GROUP BY status")))
        states["pages"] = self.db.execute("SELECT count(*) FROM pages").fetchone()[0]
        states["unique_contents"] = self.db.execute("SELECT count(DISTINCT content_hash) FROM pages").fetchone()[0]
        return states

    def export(self, path: str | Path, *, unique: bool = False) -> int:
        target = Path(path).resolve()
        protected = {self.path, Path(str(self.path) + ".lock"), Path(str(self.path) + "-wal"), Path(str(self.path) + "-shm")}
        if target in protected:
            raise ValueError("Export must not overwrite the database or its sidecar files")
        target.parent.mkdir(parents=True, exist_ok=True)
        seen = set()
        count = 0
        with target.open("w", encoding="utf-8") as out:
            for row in self.db.execute("""
                SELECT p.*, c.content, t.status AS task_status, t.error AS last_error
                FROM pages p JOIN contents c ON p.content_hash=c.hash
                JOIN tasks t ON p.url=t.url ORDER BY p.url
            """):
                data = dict(row)
                if unique and data["content_hash"] in seen:
                    continue
                seen.add(data["content_hash"])
                data["changed"] = bool(data["changed"])
                out.write(json.dumps(data, ensure_ascii=False) + "\n")
                count += 1
        return count
