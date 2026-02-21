#!/usr/bin/env python3
"""
Data Recorder - Continuous recording of BTC prices and Polymarket data.
Analog of the paper trading system but without strategy execution.
"""

import asyncio
import sys
import os
import ctypes
import time
import threading
import queue
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List
from pathlib import Path
from collections import deque

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from data.clients import BinanceClient, CLOBClient, RTDSClient, MarketDiscovery, MarketDiscovery5m
from data.polymarket_target_api import PolymarketTargetPriceAPI
from db.database import TradingDatabase

import builtins

def disable_quick_edit():
    """Disable QuickEdit mode and enable ANSI support in Windows console"""
    if os.name == 'nt':
        try:
            kernel32 = ctypes.windll.kernel32
            # Handle for Stdin (-10)
            h_stdin = kernel32.GetStdHandle(-10)
            mode = ctypes.c_uint()
            kernel32.GetConsoleMode(h_stdin, ctypes.byref(mode))
            
            # ENABLE_QUICK_EDIT_MODE = 0x0040
            # ENABLE_EXTENDED_FLAGS = 0x0080
            # To disable QuickEdit, we must clear 0x0040 and ENSURE 0x0080 is set
            new_mode = mode.value & ~0x0040
            new_mode |= 0x0080 
            kernel32.SetConsoleMode(h_stdin, new_mode)
            
            # Handle for Stdout (-11) to enable ANSI (\033 codes)
            h_stdout = kernel32.GetStdHandle(-11)
            o_mode = ctypes.c_uint()
            kernel32.GetConsoleMode(h_stdout, ctypes.byref(o_mode))
            # 0x0004: ENABLE_VIRTUAL_TERMINAL_PROCESSING
            kernel32.SetConsoleMode(h_stdout, o_mode.value | 0x0004)
        except Exception:
            pass

class DBWriter(threading.Thread):
    """Background thread for non-blocking database writes"""
    def __init__(self):
        super().__init__(daemon=True)
        self.queue = queue.Queue()
        self.running = True
        self._db = None
        self._db_path = None
        
    def set_db(self, db_instance, path):
        self._db = db_instance
        self._db_path = path

    def run(self):
        while self.running or not self.queue.empty():
            try:
                task = self.queue.get(timeout=1)
                func_name, args = task
                
                if self._db:
                    func = getattr(self._db, func_name)
                    func(*args)
                
                self.queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                # Use standard print here as it's a background thread
                sys.stderr.write(f"\n[DB_WRITER_ERROR] {e}\n")

    def add_task(self, func_name, *args):
        self.queue.put((func_name, args))

    def stop(self):
        self.running = False
        self.join(timeout=5)

class DataRecorder:
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.db_dir = os.path.join(self.base_dir, "db")
        os.makedirs(self.db_dir, exist_ok=True)
        self.heartbeat_path = os.path.join(self.base_dir, "recorder_heartbeat.txt")
        
        # State
        self.running = False
        self.start_time = datetime.now(timezone.utc)
        self.db_date = None
        self.db = None
        self.db_writer = DBWriter()
        
        # Market Data - 15m
        self.current_market = None
        self.market_history = {} # slug -> token_ids cache for resolution matching
        self.binance_price = None
        self.oracle_price = None
        self.target_price = None
        self.up_prices = None
        self.down_prices = None
        
        # Market Data - 5m
        self.current_market_5m = None
        self.market_history_5m = {}
        self.target_price_5m = None
        self.up_prices_5m = None
        self.down_prices_5m = None
        
        # BTC price recording throttle (record ~3Hz, not every tick)
        self.last_btc_record_ts = 0
        
        # Health & Latency
        self.errors = []
        self.last_update_ts = {
            'binance': 0,
            'oracle': 0,
            'clob': 0,
            'clob_5m': 0
        }
        self.bnc_history = deque(maxlen=100000) # Increased for consistent history across bots
        self.current_lag_ms = 0
        
        # Clients
        self.binance_client = BinanceClient(self.on_binance_update)
        self.clob_client = CLOBClient(self.on_clob_update, self.on_market_resolved)
        self.rtds_client = RTDSClient(self.on_rtds_update)
        self.polymarket_api = PolymarketTargetPriceAPI()
        
    def on_binance_update(self, price: float):
        self.binance_price = price
        now = time.time()
        self.last_update_ts['binance'] = now
        self.bnc_history.append((now, price))
        
    def on_clob_update(self, market_slug: str, up_prices: Dict, down_prices: Dict):
        if self.current_market and market_slug == self.current_market['slug']:
            self.up_prices = up_prices
            self.down_prices = down_prices
            self.last_update_ts['clob'] = time.time()
        if self.current_market_5m and market_slug == self.current_market_5m['slug']:
            self.up_prices_5m = up_prices
            self.down_prices_5m = down_prices
            self.last_update_ts['clob_5m'] = time.time()
            
    def on_rtds_update(self, oracle_price: float):
        self.oracle_price = oracle_price
        now = time.time()
        self.last_update_ts['oracle'] = now
        
        # Calculate lag: find when this price first appeared on Binance
        if self.bnc_history:
            # Simple correlation: find first BNC price within 10s that matches ORC
            # In practice, oracle might be slightly different, so we find closest match
            best_match_ts = None
            min_diff = float('inf')
            
            for b_ts, b_price in reversed(self.bnc_history):
                # Only look back up to 10 seconds
                if now - b_ts > 10: break
                
                diff = abs(b_price - oracle_price)
                if diff < min_diff:
                    min_diff = diff
                    best_match_ts = b_ts
                
                # If we find exact match (or very close), stop
                if diff < 0.1:
                    break
            
            if best_match_ts:
                self.current_lag_ms = int((now - best_match_ts) * 1000)

    def on_market_resolved(self, market_slug: str, winning_asset_id: str):
        winner = "Unknown"
        token_ids = []
        
        # 1. Try to get tokens from history (most reliable for delayed events)
        if market_slug in self.market_history:
            token_ids = self.market_history[market_slug]
        elif market_slug in self.market_history_5m:
            token_ids = self.market_history_5m[market_slug]
        # 2. Fallback to current market
        elif self.current_market and self.current_market['slug'] == market_slug:
            token_ids = self.current_market.get('token_ids', [])
        elif self.current_market_5m and self.current_market_5m['slug'] == market_slug:
            token_ids = self.current_market_5m.get('token_ids', [])
            
        if len(token_ids) >= 2:
            if winning_asset_id == token_ids[0]:
                winner = "UP"
            elif winning_asset_id == token_ids[1]:
                winner = "DOWN"
        
        market_type = "5m" if "5m" in market_slug else "15m"
        msg = f"[{market_type}] Market Resolved: {market_slug} | Winner: {winner} ({winning_asset_id})"
        self.log_event("market_outcome", msg)

    def log_event(self, event_type: str, message: str):
        if self.db:
            try:
                self.db_writer.add_task("log_event", event_type, message)
            except Exception as e:
                self.errors.append(f"DB Log Queue Error: {e}")
        
        # Output to console
        curr_time = datetime.now().strftime('%H:%M:%S')
        sys.stdout.write("\r" + " " * 125 + "\r")
        sys.stdout.write(f"[{curr_time}] [{event_type.upper()}] {message}\n")
        sys.stdout.flush()

    async def check_db_rotation(self):
        now_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if now_date != self.db_date:
            if self.db:
                try:
                    # Wait for queue to drain before closing
                    while not self.db_writer.queue.empty():
                        await asyncio.sleep(0.1)
                    self.db.close()
                except:
                    pass
            
            self.db_date = now_date
            data_dir = os.path.join(self.db_dir, f"recorder_{self.db_date}")
            self.db = TradingDatabase(data_dir)
            self.db_writer.set_db(self.db, data_dir)
            self.log_event("system", f"Started new data directory: recorder_{self.db_date}")

    async def update_market_discovery(self):
        try:
            market = await asyncio.to_thread(MarketDiscovery.get_current_market)
            if market and (not self.current_market or market['slug'] != self.current_market['slug']):
                self.current_market = market
                slug = market['slug']
                token_ids = market['token_ids']
                
                # Save to history for resolution matching
                self.market_history[slug] = token_ids
                if len(self.market_history) > 20:
                    oldest_slug = next(iter(self.market_history))
                    del self.market_history[oldest_slug]
                
                self.clob_client.set_market(token_ids, slug)
                self.target_price = await asyncio.to_thread(self.polymarket_api.get_target_price, slug)
                self.log_event("market_switch", f"[15m] New market: {slug} (Strike: ${self.target_price})")
        except Exception as e:
            self.errors.append(f"Discovery 15m Error: {e}")

    async def update_market_discovery_5m(self):
        try:
            market = await asyncio.to_thread(MarketDiscovery5m.get_current_market)
            if market and (not self.current_market_5m or market['slug'] != self.current_market_5m['slug']):
                self.current_market_5m = market
                slug = market['slug']
                token_ids = market['token_ids']
                
                self.market_history_5m[slug] = token_ids
                if len(self.market_history_5m) > 40:
                    oldest_slug = next(iter(self.market_history_5m))
                    del self.market_history_5m[oldest_slug]
                
                self.clob_client.set_market(token_ids, slug)
                self.target_price_5m = await asyncio.to_thread(self.polymarket_api.get_target_price, slug)
                self.log_event("market_switch", f"[5m] New market: {slug} (Strike: ${self.target_price_5m})")
        except Exception as e:
            self.errors.append(f"Discovery 5m Error: {e}")

    async def record_snapshot(self):
        if not self.db:
            return

        now_utc = datetime.now(timezone.utc)
        now_ts = time.time()
        ts_iso = now_utc.isoformat()

        # Record BTC prices (~3Hz, shared for both market types)
        if now_ts - self.last_btc_record_ts >= 0.3 and (self.binance_price or self.oracle_price):
            self.db_writer.add_task(
                "insert_btc_price", ts_iso,
                self.binance_price, self.oracle_price, self.current_lag_ms
            )
            self.last_btc_record_ts = now_ts

        # Record 15m market snapshot
        if self.current_market:
            try:
                snapshot = {
                    "timestamp": ts_iso,
                    "market_slug": self.current_market['slug'],
                    "oracle_price": self.oracle_price,
                    "binance_price": self.binance_price,
                    "up_bid": self.up_prices.get("bid", 0) if self.up_prices else 0,
                    "up_ask": self.up_prices.get("ask", 0) if self.up_prices else 0,
                    "up_mid": self.up_prices.get("mid", 0) if self.up_prices else 0,
                    "down_bid": self.down_prices.get("bid", 0) if self.down_prices else 0,
                    "down_ask": self.down_prices.get("ask", 0) if self.down_prices else 0,
                    "down_mid": self.down_prices.get("mid", 0) if self.down_prices else 0,
                    "time_to_expiry": int((self.current_market['end_time'] - now_utc).total_seconds()) if 'end_time' in self.current_market else 0,
                    "metadata": {
                        "target_price": self.target_price,
                        "recorder": True,
                        "lag_ms": self.current_lag_ms
                    }
                }
                self.db_writer.add_task("insert_market_snapshot", snapshot)
            except Exception as e:
                self.errors.append(f"15m Snapshot Error: {e}")

        # Record 5m market snapshot
        if self.current_market_5m:
            try:
                snapshot_5m = {
                    "timestamp": ts_iso,
                    "market_slug": self.current_market_5m['slug'],
                    "oracle_price": self.oracle_price,
                    "binance_price": self.binance_price,
                    "up_bid": self.up_prices_5m.get("bid", 0) if self.up_prices_5m else 0,
                    "up_ask": self.up_prices_5m.get("ask", 0) if self.up_prices_5m else 0,
                    "up_mid": self.up_prices_5m.get("mid", 0) if self.up_prices_5m else 0,
                    "down_bid": self.down_prices_5m.get("bid", 0) if self.down_prices_5m else 0,
                    "down_ask": self.down_prices_5m.get("ask", 0) if self.down_prices_5m else 0,
                    "down_mid": self.down_prices_5m.get("mid", 0) if self.down_prices_5m else 0,
                    "time_to_expiry": int((self.current_market_5m['end_time'] - now_utc).total_seconds()) if 'end_time' in self.current_market_5m else 0,
                    "metadata": {
                        "target_price": self.target_price_5m,
                        "recorder": True,
                        "lag_ms": self.current_lag_ms
                    }
                }
                self.db_writer.add_task("insert_market_snapshot_5m", snapshot_5m)
            except Exception as e:
                self.errors.append(f"5m Snapshot Error: {e}")

    async def update_heartbeat(self):
        """Update heartbeat file to show we're alive even if console is frozen"""
        try:
            with open(self.heartbeat_path, "w") as f:
                f.write(datetime.now(timezone.utc).isoformat())
        except:
            pass

    def display_status(self):
        now = datetime.now(timezone.utc)
        elapsed = now - self.start_time
        hours, remainder = divmod(int(elapsed.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        curr_time = now.strftime("%H:%M:%S")
        
        # Latency calculation
        curr_ts = time.time()
        
        # 15m market info
        up_str = f"U:{self.up_prices['bid']:.3f}/{self.up_prices['ask']:.3f}" if self.up_prices else "U:---"
        down_str = f"D:{self.down_prices['bid']:.3f}/{self.down_prices['ask']:.3f}" if self.down_prices else "D:---"
        mkt_15m = self.current_market['slug'][-15:] if self.current_market else 'None'
        
        # 5m market info
        up5_str = f"U:{self.up_prices_5m['bid']:.3f}/{self.up_prices_5m['ask']:.3f}" if self.up_prices_5m else "U:---"
        down5_str = f"D:{self.down_prices_5m['bid']:.3f}/{self.down_prices_5m['ask']:.3f}" if self.down_prices_5m else "D:---"
        mkt_5m = self.current_market_5m['slug'][-14:] if self.current_market_5m else 'None'
        
        status_line = (
            f"[{curr_time}] {hours}h{minutes}m | "
            f"BNC:{self.binance_price or 0:>8.1f} ORC:{self.oracle_price or 0:>8.1f} LAG:{self.current_lag_ms:>4}ms | "
            f"15m:{mkt_15m} {up_str} {down_str} | "
            f"5m:{mkt_5m} {up5_str} {down5_str}"
        )
        
        sys.stdout.write(f"\r{status_line:<160}")
        sys.stdout.flush()
        
        if self.errors:
            err = self.errors.pop(0)
            sys.stdout.write(f"\n[ERROR] {err}\n")
            sys.stdout.flush()

    async def connection_health_monitor(self):
        """Monitor connection health and alert on issues (like in original script)"""
        await asyncio.sleep(30)
        while self.running:
            await asyncio.sleep(60)
            now = time.time()
            issues = []
            
            if self.last_update_ts['binance'] > 0:
                silence = now - self.last_update_ts['binance']
                if silence > 60:
                    issues.append(f"Binance silent for {silence:.0f}s")
            
            if self.last_update_ts['oracle'] > 0:
                silence = now - self.last_update_ts['oracle']
                if silence > 60:
                    issues.append(f"RTDS (Oracle) silent for {silence:.0f}s")
            
            if self.last_update_ts['clob'] > 0:
                silence = now - self.last_update_ts['clob']
                if silence > 60:
                    issues.append(f"CLOB silent for {silence:.0f}s")
            
            if issues:
                msg = "Connection issues: " + " | ".join(issues)
                self.log_event("health", msg)

    async def start(self):
        disable_quick_edit()
        
        self.running = True
        print("Starting Data Recorder (15m + 5m markets, with Background JSON Writer)...")
        
        # Start DB writer thread
        self.db_writer.start()
        
        # Start clients
        asyncio.create_task(self.binance_client.start())
        asyncio.create_task(self.rtds_client.start())
        asyncio.create_task(self.clob_client.start())
        
        # Start health monitor
        asyncio.create_task(self.connection_health_monitor())
        
        last_discovery_15m = 0
        last_discovery_5m = 0
        last_heartbeat = 0
        
        try:
            while self.running:
                await self.check_db_rotation()
                
                now_ts = time.time()

                # 15m market discovery every 60s
                if now_ts - last_discovery_15m > 60:
                    await self.update_market_discovery()
                    last_discovery_15m = now_ts

                # 5m market discovery every 30s (shorter markets need faster discovery)
                if now_ts - last_discovery_5m > 30:
                    await self.update_market_discovery_5m()
                    last_discovery_5m = now_ts
                
                # Heartbeat every 30s
                if now_ts - last_heartbeat > 30:
                    await self.update_heartbeat()
                    last_heartbeat = now_ts

                await self.record_snapshot()
                self.display_status()
                
                # ~3Hz recording
                await asyncio.sleep(0.33)
                
        except (KeyboardInterrupt, asyncio.CancelledError):
            print("\nStopping...")
        finally:
            self.running = False
            # Wait for DB writer to finish
            print("\nWaiting for JSON writer to finish...")
            self.db_writer.stop()
            if self.db:
                self.db.close()
            print("Shutdown complete.")

if __name__ == "__main__":
    recorder = DataRecorder()
    asyncio.run(recorder.start())
