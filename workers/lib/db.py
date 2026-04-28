from __future__ import annotations

import os
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Sequence


DEFAULT_DB_PATH = Path("/tmp/kworb_italy.db")
SQLITE_BUSY_TIMEOUT_MS = 5000

_NAMED_PARAM_RE = re.compile(r"(?<!:):([A-Za-z_]\w*)")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def get_db_path() -> Path:
    raw_path = os.environ.get("DB_PATH")
    if raw_path:
        return Path(raw_path)
    return DEFAULT_DB_PATH


def get_database_url() -> str | None:
    return os.environ.get("DATABASE_URL")


def is_postgres() -> bool:
    url = get_database_url()
    return bool(url and url.startswith("postgresql://"))


class _FakeCursor:
    """No-op cursor returned for PRAGMA statements when using Postgres."""

    def fetchall(self) -> list:
        return []

    def fetchone(self) -> None:
        return None

    def __iter__(self) -> Iterator:
        return iter([])


def _adapt_sql(sql: str) -> str:
    """Translate SQLite parameter syntax to psycopg2 syntax."""
    # Named :param → %(param)s  (negative lookbehind avoids ::cast)
    sql = _NAMED_PARAM_RE.sub(r"%(\1)s", sql)
    # Positional ? → %s
    sql = sql.replace("?", "%s")
    return sql


class DbAdapter:
    """Uniform wrapper over sqlite3 or psycopg2 connections."""

    def __init__(self, raw: Any, backend: str) -> None:
        self._conn = raw
        self.backend = backend  # "sqlite" | "postgres"

    def execute(self, sql: str, params: Any = None) -> Any:
        if self.backend == "postgres":
            if sql.lstrip().upper().startswith("PRAGMA"):
                return _FakeCursor()
            cur = self._conn.cursor()
            cur.execute(_adapt_sql(sql), params or ())
            return cur
        return self._conn.execute(sql, params or ())

    def executemany(self, sql: str, seq: Sequence[Any]) -> None:
        if self.backend == "postgres":
            cur = self._conn.cursor()
            cur.executemany(_adapt_sql(sql), seq)
        else:
            self._conn.executemany(sql, seq)

    def executescript(self, sql_text: str) -> None:
        if self.backend == "postgres":
            cur = self._conn.cursor()
            for chunk in sql_text.split(";"):
                lines = [
                    ln for ln in chunk.splitlines()
                    if ln.strip() and not ln.strip().startswith("--")
                ]
                if lines:
                    cur.execute("\n".join(lines))
        else:
            self._conn.executescript(sql_text)

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "DbAdapter":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        self.close()


def connect(db_path: Path | None = None) -> DbAdapter:
    if is_postgres():
        import psycopg2
        import psycopg2.extras
        raw = psycopg2.connect(
            get_database_url(),
            cursor_factory=psycopg2.extras.DictCursor,
        )
        return DbAdapter(raw, "postgres")
    resolved = (db_path or get_db_path()).expanduser()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    raw = sqlite3.connect(resolved)
    raw.row_factory = sqlite3.Row
    raw.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
    raw.execute("PRAGMA journal_mode = WAL")
    raw.execute("PRAGMA synchronous = NORMAL")
    return DbAdapter(raw, "sqlite")


def table_columns(conn: DbAdapter, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row[1] for row in rows}


def ensure_track_preview_columns(conn: DbAdapter) -> None:
    statements = [
        "ALTER TABLE tracks ADD COLUMN preview_url TEXT",
        "ALTER TABLE tracks ADD COLUMN preview_fetched TEXT",
        "ALTER TABLE tracks ADD COLUMN preview_status TEXT",
    ]
    for statement in statements:
        try:
            conn.execute(statement)
        except sqlite3.OperationalError as exc:
            if "duplicate column name" not in str(exc).lower():
                raise


def load_sql_file(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def run_migrations(conn: DbAdapter, sql_text: str) -> None:
    if conn.backend == "sqlite":
        ensure_track_preview_columns(conn)
    conn.executescript(sql_text)
    conn.commit()


@contextmanager
def transaction(conn: DbAdapter) -> Iterator[DbAdapter]:
    try:
        if conn.backend == "sqlite":
            conn.execute("BEGIN")
        yield conn
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()


def fetch_track_ids(
    conn: DbAdapter,
    query: str,
    params: Sequence[object] | None = None,
) -> list[str]:
    rows = conn.execute(query, params or ()).fetchall()
    return [row[0] for row in rows]
