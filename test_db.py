#!/usr/bin/env python3
"""Tests for db.py – DBWriter and read helpers."""

import json
import os
import sqlite3
import tempfile
import time
import unittest

from db import DBWriter, DB_FILENAME, read_db, get_summary_stats, list_dates, get_time_range, get_market_slugs


class TestDBWriter(unittest.TestCase):
    """Test the background DBWriter thread."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.data_dir = os.path.join(self.tmpdir, "2025-01-15")

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _make_writer(self):
        w = DBWriter()
        w.start()
        w.set_data_dir(self.data_dir)
        return w

    def _stop(self, w: DBWriter):
        w.stop()

    # -- basic insert & read ------------------------------------------------

    def test_write_btc_prices(self):
        w = self._make_writer()
        w.add("btc_prices", {
            "timestamp": "2025-01-15T10:00:00+00:00",
            "binance_price": 42000.5,
            "oracle_price": 41999.0,
            "lag_ms": 120,
        })
        self._stop(w)

        db_path = os.path.join(self.data_dir, DB_FILENAME)
        self.assertTrue(os.path.exists(db_path))

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM btc_prices").fetchall()
        conn.close()

        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["binance_price"], 42000.5)
        self.assertEqual(rows[0]["lag_ms"], 120)

    def test_write_market_snapshot(self):
        w = self._make_writer()
        w.add("market_snapshots_15m", {
            "timestamp": "2025-01-15T10:00:00+00:00",
            "market_slug": "btc-updown-15m-test",
            "oracle_price": 42000.0,
            "binance_price": 42001.0,
            "up_bid": 0.62, "up_ask": 0.63, "up_mid": 0.625,
            "down_bid": 0.37, "down_ask": 0.38, "down_mid": 0.375,
            "time_to_expiry": 300,
            "target_price": 42500.0,
            "lag_ms": 100,
        })
        self._stop(w)

        rows = read_db(self.tmpdir, "2025-01-15", "market_snapshots_15m")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["market_slug"], "btc-updown-15m-test")
        self.assertAlmostEqual(rows[0]["up_bid"], 0.62)

    def test_write_orderbook_json_columns(self):
        """Orderbook bid/ask arrays should be serialised as JSON in SQLite
        and deserialised back to lists when read."""
        w = self._make_writer()
        bids = [{"price": 0.62, "size": 1000}, {"price": 0.61, "size": 500}]
        asks = [{"price": 0.63, "size": 800}]
        w.add("orderbook_15m", {
            "timestamp": "2025-01-15T10:00:00+00:00",
            "market_slug": "btc-updown-15m-test",
            "up_bids": bids,
            "up_asks": asks,
            "down_bids": [],
            "down_asks": [],
            "up_bid_total": 1500.0,
            "up_ask_total": 800.0,
            "down_bid_total": 0.0,
            "down_ask_total": 0.0,
        })
        self._stop(w)

        rows = read_db(self.tmpdir, "2025-01-15", "orderbook_15m")
        self.assertEqual(len(rows), 1)
        self.assertIsInstance(rows[0]["up_bids"], list)
        self.assertEqual(len(rows[0]["up_bids"]), 2)
        self.assertEqual(rows[0]["up_bids"][0]["price"], 0.62)

    def test_write_system_events(self):
        w = self._make_writer()
        w.add("system_events", {
            "timestamp": "2025-01-15T10:00:00+00:00",
            "event_type": "market_switch",
            "message": "New market started",
        })
        self._stop(w)

        rows = read_db(self.tmpdir, "2025-01-15", "system_events")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["event_type"], "market_switch")

    def test_multiple_records(self):
        w = self._make_writer()
        for i in range(50):
            w.add("btc_prices", {
                "timestamp": f"2025-01-15T10:00:{i:02d}+00:00",
                "binance_price": 42000.0 + i,
                "oracle_price": 41999.0 + i,
                "lag_ms": 100 + i,
            })
        self._stop(w)

        rows = read_db(self.tmpdir, "2025-01-15", "btc_prices")
        self.assertEqual(len(rows), 50)

    def test_date_rotation(self):
        """Switching data_dir should close old DB and open a new one."""
        w = self._make_writer()
        w.add("btc_prices", {
            "timestamp": "2025-01-15T23:59:59+00:00",
            "binance_price": 42000.0,
            "oracle_price": 41999.0,
            "lag_ms": 100,
        })

        day2 = os.path.join(self.tmpdir, "2025-01-16")
        w.set_data_dir(day2)
        w.add("btc_prices", {
            "timestamp": "2025-01-16T00:00:01+00:00",
            "binance_price": 42100.0,
            "oracle_price": 42099.0,
            "lag_ms": 110,
        })
        self._stop(w)

        rows_d1 = read_db(self.tmpdir, "2025-01-15", "btc_prices")
        rows_d2 = read_db(self.tmpdir, "2025-01-16", "btc_prices")
        self.assertEqual(len(rows_d1), 1)
        self.assertEqual(len(rows_d2), 1)
        self.assertAlmostEqual(rows_d2[0]["binance_price"], 42100.0)


class TestReadDB(unittest.TestCase):
    """Test read_db helper."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.date = "2025-02-01"
        self.date_dir = os.path.join(self.tmpdir, self.date)
        os.makedirs(self.date_dir)
        self.db_path = os.path.join(self.date_dir, DB_FILENAME)

        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE btc_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT, binance_price REAL, oracle_price REAL, lag_ms INTEGER
            )
        """)
        for i in range(100):
            conn.execute(
                "INSERT INTO btc_prices (timestamp, binance_price, oracle_price, lag_ms) VALUES (?,?,?,?)",
                (f"2025-02-01T10:00:{i:02d}+00:00", 50000.0 + i, 49999.0 + i, 80 + i),
            )
        conn.commit()
        conn.close()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_read_all(self):
        rows = read_db(self.tmpdir, self.date, "btc_prices")
        self.assertEqual(len(rows), 100)
        # id column should be stripped
        self.assertNotIn("id", rows[0])

    def test_read_with_limit(self):
        rows = read_db(self.tmpdir, self.date, "btc_prices", limit=10)
        self.assertEqual(len(rows), 10)
        # Last element should be the actual last record
        self.assertAlmostEqual(rows[-1]["binance_price"], 50099.0)

    def test_read_nonexistent_table(self):
        with self.assertRaises(ValueError):
            read_db(self.tmpdir, self.date, "nonexistent_table")

    def test_read_nonexistent_date(self):
        rows = read_db(self.tmpdir, "1999-01-01", "btc_prices")
        self.assertEqual(rows, [])


class TestGetSummaryStats(unittest.TestCase):
    """Test get_summary_stats helper."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.date = "2025-03-01"
        self.date_dir = os.path.join(self.tmpdir, self.date)
        os.makedirs(self.date_dir)
        self.db_path = os.path.join(self.date_dir, DB_FILENAME)

        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE btc_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT, binance_price REAL, oracle_price REAL, lag_ms INTEGER
            )
        """)
        conn.execute("INSERT INTO btc_prices VALUES (NULL, '2025-03-01T10:00:00', 50000.0, 49999.0, 100)")
        conn.execute("INSERT INTO btc_prices VALUES (NULL, '2025-03-01T10:00:01', 50100.0, 50099.0, 200)")
        conn.execute("INSERT INTO btc_prices VALUES (NULL, '2025-03-01T10:00:02', 49900.0, 49899.0, 150)")

        conn.execute("""
            CREATE TABLE market_snapshots_15m (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT, market_slug TEXT, oracle_price REAL, binance_price REAL,
                up_bid REAL, up_ask REAL, up_mid REAL,
                down_bid REAL, down_ask REAL, down_mid REAL,
                time_to_expiry INTEGER, target_price REAL, lag_ms INTEGER
            )
        """)
        conn.execute(
            "INSERT INTO market_snapshots_15m VALUES (NULL, '2025-03-01T10:00:00', 'slug-a', 50000, 50001, 0.6, 0.61, 0.605, 0.39, 0.4, 0.395, 300, 50500, 100)"
        )
        conn.execute(
            "INSERT INTO market_snapshots_15m VALUES (NULL, '2025-03-01T10:00:01', 'slug-b', 50100, 50101, 0.7, 0.71, 0.705, 0.29, 0.3, 0.295, 280, 50500, 200)"
        )

        conn.execute("""
            CREATE TABLE system_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT, event_type TEXT, message TEXT
            )
        """)
        conn.execute("INSERT INTO system_events VALUES (NULL, '2025-03-01T10:00:00', 'system', 'started')")

        conn.commit()
        conn.close()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_summary(self):
        s = get_summary_stats(self.tmpdir, self.date)
        self.assertEqual(s["date"], self.date)

        self.assertEqual(s["btc"]["count"], 3)
        self.assertAlmostEqual(s["btc"]["min"], 49900.0)
        self.assertAlmostEqual(s["btc"]["max"], 50100.0)
        self.assertAlmostEqual(s["btc"]["first"], 50000.0)
        self.assertAlmostEqual(s["btc"]["last"], 49900.0)
        self.assertEqual(s["btc"]["avg_lag_ms"], 150)

        self.assertEqual(s["markets_15m"]["snapshot_count"], 2)
        self.assertEqual(s["markets_15m"]["unique_markets"], 2)

        self.assertEqual(s["events"], 1)

    def test_summary_nonexistent(self):
        s = get_summary_stats(self.tmpdir, "1999-01-01")
        self.assertEqual(s, {})


class TestListDates(unittest.TestCase):
    """Test list_dates helper."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        # Create a date directory with a DB
        d1 = os.path.join(self.tmpdir, "2025-01-10")
        os.makedirs(d1)
        conn = sqlite3.connect(os.path.join(d1, DB_FILENAME))
        conn.execute("CREATE TABLE btc_prices (id INTEGER PRIMARY KEY, timestamp TEXT)")
        conn.commit()
        conn.close()

        # Create a date directory with legacy JSONL files only
        d2 = os.path.join(self.tmpdir, "2025-01-09")
        os.makedirs(d2)
        with open(os.path.join(d2, "btc_prices.jsonl"), "w") as f:
            f.write('{"timestamp":"2025-01-09T10:00:00"}\n')

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_list(self):
        dates = list_dates(self.tmpdir)
        self.assertEqual(len(dates), 2)
        # Should be reverse-sorted
        self.assertEqual(dates[0]["date"], "2025-01-10")
        self.assertIn("btc_prices", dates[0]["files"])
        self.assertEqual(dates[1]["date"], "2025-01-09")
        self.assertIn("btc_prices", dates[1]["files"])

    def test_empty_dir(self):
        empty = tempfile.mkdtemp()
        dates = list_dates(empty)
        self.assertEqual(dates, [])
        import shutil
        shutil.rmtree(empty, ignore_errors=True)


class TestReadDBFilters(unittest.TestCase):
    """Test read_db time-range and market_slug filtering."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.date = "2025-04-01"
        self.date_dir = os.path.join(self.tmpdir, self.date)
        os.makedirs(self.date_dir)
        self.db_path = os.path.join(self.date_dir, DB_FILENAME)

        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE btc_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT, binance_price REAL, oracle_price REAL, lag_ms INTEGER
            )
        """)
        # Insert records at 10:00, 10:30, 11:00, 11:30, 12:00
        for h, m, price in [
            (10, 0, 50000), (10, 30, 50100), (11, 0, 50200),
            (11, 30, 50300), (12, 0, 50400),
        ]:
            conn.execute(
                "INSERT INTO btc_prices (timestamp, binance_price, oracle_price, lag_ms) VALUES (?,?,?,?)",
                (f"2025-04-01T{h:02d}:{m:02d}:00+00:00", float(price), float(price - 1), 100),
            )

        conn.execute("""
            CREATE TABLE market_snapshots_15m (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT, market_slug TEXT, oracle_price REAL, binance_price REAL,
                up_bid REAL, up_ask REAL, up_mid REAL,
                down_bid REAL, down_ask REAL, down_mid REAL,
                time_to_expiry INTEGER, target_price REAL, lag_ms INTEGER
            )
        """)
        for slug, ts in [
            ("slug-a", "2025-04-01T10:00:00+00:00"),
            ("slug-a", "2025-04-01T10:15:00+00:00"),
            ("slug-b", "2025-04-01T11:00:00+00:00"),
            ("slug-b", "2025-04-01T11:15:00+00:00"),
        ]:
            conn.execute(
                "INSERT INTO market_snapshots_15m VALUES (NULL,?,?,50000,50001,0.6,0.61,0.605,0.39,0.4,0.395,300,50500,100)",
                (ts, slug),
            )

        conn.commit()
        conn.close()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_filter_by_start_time(self):
        rows = read_db(self.tmpdir, self.date, "btc_prices",
                       start_time="2025-04-01T11:00:00+00:00")
        self.assertEqual(len(rows), 3)  # 11:00, 11:30, 12:00
        self.assertAlmostEqual(rows[0]["binance_price"], 50200.0)

    def test_filter_by_end_time(self):
        rows = read_db(self.tmpdir, self.date, "btc_prices",
                       end_time="2025-04-01T10:30:00+00:00")
        self.assertEqual(len(rows), 2)  # 10:00, 10:30

    def test_filter_by_time_range(self):
        rows = read_db(self.tmpdir, self.date, "btc_prices",
                       start_time="2025-04-01T10:30:00+00:00",
                       end_time="2025-04-01T11:30:00+00:00")
        self.assertEqual(len(rows), 3)  # 10:30, 11:00, 11:30

    def test_filter_by_market_slug(self):
        rows = read_db(self.tmpdir, self.date, "market_snapshots_15m",
                       market_slug="slug-a")
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(r["market_slug"] == "slug-a" for r in rows))

    def test_filter_by_slug_and_time(self):
        rows = read_db(self.tmpdir, self.date, "market_snapshots_15m",
                       start_time="2025-04-01T10:10:00+00:00",
                       market_slug="slug-a")
        self.assertEqual(len(rows), 1)  # only slug-a at 10:15

    def test_no_filter_returns_all(self):
        rows = read_db(self.tmpdir, self.date, "btc_prices")
        self.assertEqual(len(rows), 5)

    def test_filter_with_limit(self):
        rows = read_db(self.tmpdir, self.date, "btc_prices",
                       start_time="2025-04-01T10:00:00+00:00", limit=2)
        self.assertEqual(len(rows), 2)
        # Last element should be the last matching record
        self.assertAlmostEqual(rows[-1]["binance_price"], 50400.0)


class TestGetTimeRange(unittest.TestCase):
    """Test get_time_range helper."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.date = "2025-05-01"
        self.date_dir = os.path.join(self.tmpdir, self.date)
        os.makedirs(self.date_dir)
        self.db_path = os.path.join(self.date_dir, DB_FILENAME)

        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE btc_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT, binance_price REAL, oracle_price REAL, lag_ms INTEGER
            )
        """)
        conn.execute("INSERT INTO btc_prices VALUES (NULL, '2025-05-01T08:00:00+00:00', 50000, 49999, 100)")
        conn.execute("INSERT INTO btc_prices VALUES (NULL, '2025-05-01T20:00:00+00:00', 51000, 50999, 200)")
        conn.commit()
        conn.close()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_returns_min_max(self):
        tr = get_time_range(self.tmpdir, self.date)
        self.assertEqual(tr["min"], "2025-05-01T08:00:00+00:00")
        self.assertEqual(tr["max"], "2025-05-01T20:00:00+00:00")

    def test_nonexistent_date(self):
        tr = get_time_range(self.tmpdir, "1999-01-01")
        self.assertEqual(tr, {})


class TestGetMarketSlugs(unittest.TestCase):
    """Test get_market_slugs helper."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.date = "2025-06-01"
        self.date_dir = os.path.join(self.tmpdir, self.date)
        os.makedirs(self.date_dir)
        self.db_path = os.path.join(self.date_dir, DB_FILENAME)

        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE market_snapshots_15m (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT, market_slug TEXT, oracle_price REAL, binance_price REAL,
                up_bid REAL, up_ask REAL, up_mid REAL,
                down_bid REAL, down_ask REAL, down_mid REAL,
                time_to_expiry INTEGER, target_price REAL, lag_ms INTEGER
            )
        """)
        conn.execute("INSERT INTO market_snapshots_15m VALUES (NULL, '2025-06-01T10:00:00', 'slug-c', 50000, 50001, 0.6, 0.61, 0.605, 0.39, 0.4, 0.395, 300, 50500, 100)")
        conn.execute("INSERT INTO market_snapshots_15m VALUES (NULL, '2025-06-01T10:15:00', 'slug-a', 50000, 50001, 0.6, 0.61, 0.605, 0.39, 0.4, 0.395, 300, 50500, 100)")
        conn.execute("INSERT INTO market_snapshots_15m VALUES (NULL, '2025-06-01T10:30:00', 'slug-b', 50000, 50001, 0.6, 0.61, 0.605, 0.39, 0.4, 0.395, 300, 50500, 100)")
        conn.execute("INSERT INTO market_snapshots_15m VALUES (NULL, '2025-06-01T10:45:00', 'slug-a', 50000, 50001, 0.6, 0.61, 0.605, 0.39, 0.4, 0.395, 300, 50500, 100)")
        conn.commit()
        conn.close()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_returns_unique_sorted(self):
        slugs = get_market_slugs(self.tmpdir, self.date, "15m")
        self.assertEqual(slugs, ["slug-a", "slug-b", "slug-c"])

    def test_nonexistent_date(self):
        slugs = get_market_slugs(self.tmpdir, "1999-01-01", "15m")
        self.assertEqual(slugs, [])


if __name__ == "__main__":
    unittest.main()
