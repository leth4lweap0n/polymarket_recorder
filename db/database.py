#!/usr/bin/env python3
"""
JSON-based storage for trading data recorder.

Uses JSONL (JSON Lines) format for efficient append-only writes.
Each data type is stored in a separate .jsonl file within a date-based directory.

Files:
- btc_prices.jsonl: BTC price ticks (Binance + Oracle)
- market_snapshots.jsonl: 15m market price snapshots (UP/DOWN bid/ask)
- market_snapshots_5m.jsonl: 5m market price snapshots (UP/DOWN bid/ask)
- system_events.jsonl: System events and errors
"""

import json
import os
from datetime import datetime, timezone
from typing import Dict, Optional, Any
from pathlib import Path


class TradingDatabase:
    """JSON Lines (JSONL) storage for trading data"""
    
    def __init__(self, data_dir: str = "db/recorder_data"):
        """Initialize JSON storage directory and file handles"""
        self.data_dir = data_dir
        Path(data_dir).mkdir(parents=True, exist_ok=True)
        
        # Open file handles for efficient appending
        self._files = {}
    
    def _get_file(self, name: str):
        """Get or open a file handle for the given data type"""
        if name not in self._files:
            path = os.path.join(self.data_dir, f"{name}.jsonl")
            self._files[name] = open(path, 'a', encoding='utf-8')
        return self._files[name]
    
    def _append(self, name: str, record: dict):
        """Append a JSON record as a single line to the named JSONL file"""
        f = self._get_file(name)
        f.write(json.dumps(record, ensure_ascii=False) + '\n')
        f.flush()
    
    # ==================== BTC PRICES ====================
    
    def insert_btc_price(self, timestamp: str, binance_price: float,
                         oracle_price: float = None, lag_ms: int = None):
        """Insert BTC price tick (shared between 15m and 5m)"""
        self._append('btc_prices', {
            'timestamp': timestamp,
            'binance_price': binance_price,
            'oracle_price': oracle_price,
            'lag_ms': lag_ms
        })
    
    # ==================== MARKET SNAPSHOTS ====================
    
    def _prepare_snapshot(self, snapshot: Dict[str, Any]) -> dict:
        """Prepare a snapshot record, ensuring metadata is a proper dict"""
        record = dict(snapshot)
        if 'metadata' in record and isinstance(record['metadata'], str):
            try:
                record['metadata'] = json.loads(record['metadata'])
            except (json.JSONDecodeError, TypeError):
                pass
        return record
    
    def insert_market_snapshot(self, snapshot: Dict[str, Any]):
        """Insert 15m market price snapshot"""
        self._append('market_snapshots', self._prepare_snapshot(snapshot))
    
    def insert_market_snapshot_5m(self, snapshot: Dict[str, Any]):
        """Insert 5m market price snapshot"""
        self._append('market_snapshots_5m', self._prepare_snapshot(snapshot))
    
    # ==================== SYSTEM EVENTS ====================
    
    def log_event(self, event_type: str, message: str,
                  severity: str = "INFO", metadata: Optional[Dict] = None):
        """Log system event"""
        self._append('system_events', {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'event_type': event_type,
            'severity': severity,
            'message': message,
            'metadata': metadata or {}
        })
    
    # ==================== UTILITY ====================
    
    def close(self):
        """Close all open file handles"""
        for f in self._files.values():
            try:
                f.close()
            except OSError:
                pass
        self._files.clear()


if __name__ == "__main__":
    import tempfile
    
    # Test JSON storage
    test_dir = tempfile.mkdtemp(prefix="recorder_test_")
    db = TradingDatabase(test_dir)
    
    print("JSON storage created successfully!")
    print(f"Location: {db.data_dir}")
    
    # Test inserting a BTC price
    db.insert_btc_price(
        datetime.now(timezone.utc).isoformat(),
        97500.50, 97500.30, 150
    )
    print("Inserted test BTC price")
    
    # Test inserting a market snapshot
    db.insert_market_snapshot({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "market_slug": "btc-updown-15m-test",
        "oracle_price": 97500.30,
        "binance_price": 97500.50,
        "up_bid": 0.45, "up_ask": 0.47, "up_mid": 0.46,
        "down_bid": 0.53, "down_ask": 0.55, "down_mid": 0.54,
        "time_to_expiry": 600,
        "metadata": {"target_price": 97500, "recorder": True}
    })
    print("Inserted test market snapshot")
    
    # Test logging an event
    db.log_event("system", "Test event message")
    print("Inserted test system event")
    
    # Verify files were created
    for fname in sorted(os.listdir(test_dir)):
        fpath = os.path.join(test_dir, fname)
        with open(fpath, 'r') as f:
            lines = f.readlines()
        print(f"\n{fname}: {len(lines)} record(s)")
        for line in lines:
            print(f"  {line.strip()}")
    
    db.close()
    print("\nAll tests passed!")
