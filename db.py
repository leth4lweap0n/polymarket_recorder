#!/usr/bin/env python3
"""
SQLite storage backend for Polymarket Data Recorder.
Replaces JSONL files with a single SQLite database per date directory,
providing faster reads and smaller storage.
"""

import json
import os
import sqlite3
import queue
import sys
import threading
from pathlib import Path

# ---------------------------------------------------------------------------
# Schema – one table per former JSONL file
# ---------------------------------------------------------------------------

_SCHEMAS = {
    "btc_prices": """
        CREATE TABLE IF NOT EXISTS btc_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            binance_price REAL,
            oracle_price REAL,
            lag_ms INTEGER
        )
    """,
    "market_snapshots_15m": """
        CREATE TABLE IF NOT EXISTS market_snapshots_15m (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            market_slug TEXT,
            oracle_price REAL,
            binance_price REAL,
            up_bid REAL,
            up_ask REAL,
            up_mid REAL,
            down_bid REAL,
            down_ask REAL,
            down_mid REAL,
            time_to_expiry INTEGER,
            target_price REAL,
            lag_ms INTEGER
        )
    """,
    "market_snapshots_5m": """
        CREATE TABLE IF NOT EXISTS market_snapshots_5m (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            market_slug TEXT,
            oracle_price REAL,
            binance_price REAL,
            up_bid REAL,
            up_ask REAL,
            up_mid REAL,
            down_bid REAL,
            down_ask REAL,
            down_mid REAL,
            time_to_expiry INTEGER,
            target_price REAL,
            lag_ms INTEGER
        )
    """,
    "orderbook_15m": """
        CREATE TABLE IF NOT EXISTS orderbook_15m (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            market_slug TEXT,
            up_bids TEXT,
            up_asks TEXT,
            down_bids TEXT,
            down_asks TEXT,
            up_bid_total REAL,
            up_ask_total REAL,
            down_bid_total REAL,
            down_ask_total REAL
        )
    """,
    "orderbook_5m": """
        CREATE TABLE IF NOT EXISTS orderbook_5m (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            market_slug TEXT,
            up_bids TEXT,
            up_asks TEXT,
            down_bids TEXT,
            down_asks TEXT,
            up_bid_total REAL,
            up_ask_total REAL,
            down_bid_total REAL,
            down_ask_total REAL
        )
    """,
    "system_events": """
        CREATE TABLE IF NOT EXISTS system_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            event_type TEXT,
            message TEXT
        )
    """,
}

# Columns that store JSON arrays (orderbook bid/ask lists)
_JSON_COLUMNS = {"up_bids", "up_asks", "down_bids", "down_asks"}

DB_FILENAME = "data.db"


# ---------------------------------------------------------------------------
# Writer – background thread (replaces JSONWriter)
# ---------------------------------------------------------------------------

class DBWriter(threading.Thread):
    """Background thread for non-blocking SQLite writes."""

    def __init__(self):
        super().__init__(daemon=True)
        self.queue: queue.Queue = queue.Queue()
        self.running = True
        self._db_path: str | None = None
        self._conn: sqlite3.Connection | None = None
        self._tables_created: set = set()

    # -- public API (called from main thread) --------------------------------

    def set_data_dir(self, data_dir: str):
        """Set / change output directory.  Old connection is closed first."""
        # Signal the writer thread to switch directories
        self.queue.put(("__switch_dir__", data_dir))

    def add(self, table_name: str, record: dict):
        """Enqueue a record for writing (non-blocking)."""
        self.queue.put((table_name, record))

    def stop(self):
        self.running = False
        self.join(timeout=5)
        self._close()

    # -- internal (runs on writer thread) ------------------------------------

    def run(self):
        while self.running or not self.queue.empty():
            try:
                task = self.queue.get(timeout=1)
                name, payload = task
                if name == "__switch_dir__":
                    self._do_switch_dir(payload)
                else:
                    self._write_record(name, payload)
                self.queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                sys.stderr.write(f"\n[DB_WRITER_ERROR] {e}\n")

    def _do_switch_dir(self, data_dir: str):
        self._close()
        Path(data_dir).mkdir(parents=True, exist_ok=True)
        self._db_path = os.path.join(data_dir, DB_FILENAME)
        self._conn = sqlite3.connect(self._db_path)
        # Optimise for write-heavy workload
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._tables_created.clear()

    def _ensure_table(self, table_name: str):
        if table_name not in self._tables_created:
            schema = _SCHEMAS.get(table_name)
            if schema is None:
                raise ValueError(f"Unknown table: {table_name}")
            self._conn.execute(schema)
            self._conn.commit()
            self._tables_created.add(table_name)

    def _write_record(self, table_name: str, record: dict):
        if self._conn is None:
            return
        self._ensure_table(table_name)

        # Serialise JSON-array columns
        row = {}
        for k, v in record.items():
            if k in _JSON_COLUMNS and isinstance(v, (list, dict)):
                row[k] = json.dumps(v, ensure_ascii=False)
            else:
                row[k] = v

        cols = ", ".join(row.keys())
        placeholders = ", ".join(["?"] * len(row))
        self._conn.execute(
            f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})",
            list(row.values()),
        )
        self._conn.commit()

    def _close(self):
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
        self._tables_created.clear()


# ---------------------------------------------------------------------------
# Reader helpers (used by web_ui.py)
# ---------------------------------------------------------------------------

def _db_path_for_date(snapshots_dir: str, date: str) -> str:
    return os.path.join(snapshots_dir, date, DB_FILENAME)


def read_db(snapshots_dir: str, date: str, table_name: str, limit: int | None = None) -> list[dict]:
    """Read all rows from *table_name* for a given *date*, returned as
    a list of dicts identical to the old JSONL format.

    If *limit* is given the result is evenly down-sampled (first & last kept).
    """
    db_path = _db_path_for_date(snapshots_dir, date)
    if not os.path.exists(db_path):
        return []

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        try:
            rows = conn.execute(f"SELECT * FROM {table_name} ORDER BY id").fetchall()
        except sqlite3.OperationalError:
            # Table doesn't exist
            return []

        records = []
        for row in rows:
            d = dict(row)
            d.pop("id", None)
            # Deserialise JSON-array columns
            for col in _JSON_COLUMNS:
                if col in d and isinstance(d[col], str):
                    try:
                        d[col] = json.loads(d[col])
                    except (json.JSONDecodeError, TypeError):
                        pass
            records.append(d)
    finally:
        conn.close()

    if limit and len(records) > limit:
        step = (len(records) - 1) / (limit - 1) if limit > 1 else 1
        sampled = [records[int(i * step)] for i in range(limit - 1)]
        sampled.append(records[-1])
        records = sampled

    return records


def get_summary_stats(snapshots_dir: str, date: str) -> dict:
    """Return summary statistics for a date, computed efficiently with SQL."""
    db_path = _db_path_for_date(snapshots_dir, date)
    if not os.path.exists(db_path):
        return {}

    summary: dict = {"date": date}
    conn = sqlite3.connect(db_path)
    try:
        # BTC price stats
        try:
            row = conn.execute("""
                SELECT COUNT(*) as cnt,
                       MIN(binance_price) as min_p,
                       MAX(binance_price) as max_p,
                       AVG(lag_ms) as avg_lag
                FROM btc_prices
                WHERE binance_price IS NOT NULL
            """).fetchone()
            if row and row[0]:
                first = conn.execute(
                    "SELECT binance_price FROM btc_prices WHERE binance_price IS NOT NULL ORDER BY id ASC LIMIT 1"
                ).fetchone()
                last = conn.execute(
                    "SELECT binance_price FROM btc_prices WHERE binance_price IS NOT NULL ORDER BY id DESC LIMIT 1"
                ).fetchone()
                summary["btc"] = {
                    "count": row[0],
                    "min": row[1],
                    "max": row[2],
                    "first": first[0] if first else None,
                    "last": last[0] if last else None,
                    "avg_lag_ms": round(row[3]) if row[3] is not None else None,
                }
        except sqlite3.OperationalError:
            pass

        # Market snapshot counts
        for mt in ("15m", "5m"):
            table = f"market_snapshots_{mt}"
            try:
                cnt = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                slugs = conn.execute(f"SELECT COUNT(DISTINCT market_slug) FROM {table}").fetchone()[0]
                if cnt:
                    summary[f"markets_{mt}"] = {
                        "snapshot_count": cnt,
                        "unique_markets": slugs,
                    }
            except sqlite3.OperationalError:
                pass

        # System events count
        try:
            cnt = conn.execute("SELECT COUNT(*) FROM system_events").fetchone()[0]
            if cnt:
                summary["events"] = cnt
        except sqlite3.OperationalError:
            pass
    finally:
        conn.close()

    return summary


def list_dates(snapshots_dir: str) -> list[dict]:
    """List available snapshot dates with their tables (replaces .jsonl file listing)."""
    dates = []
    if not os.path.exists(snapshots_dir):
        return dates

    for name in sorted(os.listdir(snapshots_dir), reverse=True):
        full = os.path.join(snapshots_dir, name)
        if not os.path.isdir(full):
            continue

        db_path = os.path.join(full, DB_FILENAME)
        tables: list[str] = []

        # Check for SQLite database
        if os.path.exists(db_path):
            try:
                conn = sqlite3.connect(db_path)
                rows = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
                ).fetchall()
                tables = [r[0] for r in rows if not r[0].startswith("sqlite_")]
                conn.close()
            except sqlite3.Error:
                pass

        # Also check for legacy JSONL files
        jsonl_files = [f.replace(".jsonl", "") for f in os.listdir(full) if f.endswith(".jsonl")]
        all_sources = sorted(set(tables + jsonl_files))

        if all_sources:
            dates.append({"date": name, "files": all_sources})

    return dates
