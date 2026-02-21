#!/usr/bin/env python3
"""
Database schema for paper trading system.

Tables:
- btc_prices: BTC price ticks (Binance + Oracle) - shared across 15m/5m
- market_snapshots: 15m market price snapshots (UP/DOWN bid/ask)
- market_snapshots_5m: 5m market price snapshots (UP/DOWN bid/ask)
- trades: All executed trades
- positions: Current and historical positions
- strategy_stats: Statistics per strategy variation
- system_events: System events and errors
"""

import sqlite3
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from pathlib import Path


class TradingDatabase:
    """SQLite database for paper trading data"""
    
    def __init__(self, db_path: str = "db/paper_trading.db"):
        """Initialize database connection and create tables"""
        self.db_path = db_path
        
        # Create directory if needed
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row  # Return rows as dicts
        
        # Optimize SQLite for high-frequency writes
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
        
        self.create_tables()
    
    def create_tables(self):
        """Create all database tables"""
        cursor = self.conn.cursor()
        
        # Trades table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy TEXT NOT NULL,
                variation TEXT NOT NULL,
                market_slug TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_time TEXT NOT NULL,
                entry_price REAL NOT NULL,
                entry_shares REAL NOT NULL,
                entry_cost REAL NOT NULL,
                exit_time TEXT,
                exit_price REAL,
                exit_reason TEXT,
                pnl REAL,
                pnl_pct REAL,
                balance_after REAL,
                metadata TEXT
            )
        """)
        
        # Positions table (current positions)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy TEXT NOT NULL,
                variation TEXT NOT NULL,
                market_slug TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_time TEXT NOT NULL,
                entry_price REAL NOT NULL,
                shares REAL NOT NULL,
                cost REAL NOT NULL,
                current_price REAL,
                unrealized_pnl REAL,
                last_update TEXT,
                metadata TEXT
            )
        """)
        
        # BTC prices (shared between 15m and 5m markets)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS btc_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                binance_price REAL,
                oracle_price REAL,
                lag_ms INTEGER
            )
        """)

        # 15m market snapshots (UP/DOWN prices only, BTC prices in btc_prices)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS market_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                market_slug TEXT NOT NULL,
                oracle_price REAL,
                binance_price REAL,
                up_bid REAL,
                up_ask REAL,
                up_mid REAL,
                down_bid REAL,
                down_ask REAL,
                down_mid REAL,
                time_to_expiry INTEGER,
                metadata TEXT
            )
        """)

        # 5m market snapshots
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS market_snapshots_5m (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                market_slug TEXT NOT NULL,
                oracle_price REAL,
                binance_price REAL,
                up_bid REAL,
                up_ask REAL,
                up_mid REAL,
                down_bid REAL,
                down_ask REAL,
                down_mid REAL,
                time_to_expiry INTEGER,
                metadata TEXT
            )
        """)
        
        # Strategy statistics
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS strategy_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                strategy TEXT NOT NULL,
                variation TEXT NOT NULL,
                balance REAL NOT NULL,
                total_trades INTEGER DEFAULT 0,
                winning_trades INTEGER DEFAULT 0,
                losing_trades INTEGER DEFAULT 0,
                total_pnl REAL DEFAULT 0,
                win_rate REAL DEFAULT 0,
                avg_win REAL DEFAULT 0,
                avg_loss REAL DEFAULT 0,
                max_drawdown REAL DEFAULT 0,
                sharpe_ratio REAL DEFAULT 0,
                metadata TEXT
            )
        """)
        
        # System events
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                event_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                message TEXT NOT NULL,
                metadata TEXT
            )
        """)
        
        # Portfolios table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS portfolios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                strategy_name TEXT NOT NULL,
                type TEXT NOT NULL, -- Aggressive, Neutral, Conservative
                initial_capital REAL NOT NULL,
                current_balance REAL NOT NULL,
                free_funds REAL NOT NULL,
                locked_funds REAL DEFAULT 0,
                locked_until TEXT,
                min_bet REAL NOT NULL,
                max_bet REAL NOT NULL,
                bet_percentage REAL NOT NULL,
                status TEXT DEFAULT 'active', -- active, stopped
                created_at TEXT NOT NULL,
                metadata TEXT
            )
        """)

        # Portfolio trades table (link portfolios to trades)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS portfolio_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portfolio_id INTEGER NOT NULL,
                trade_id INTEGER NOT NULL,
                FOREIGN KEY (portfolio_id) REFERENCES portfolios(id),
                FOREIGN KEY (trade_id) REFERENCES trades(id)
            )
        """)

        # Portfolio withdrawals
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS portfolio_withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portfolio_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                timestamp TEXT NOT NULL,
                FOREIGN KEY (portfolio_id) REFERENCES portfolios(id)
            )
        """)

        # Create indices for faster queries
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_strategy ON trades(strategy, variation)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_time ON trades(entry_time)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_strategy ON positions(strategy, variation)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_time ON market_snapshots(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_5m_time ON market_snapshots_5m(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_5m_slug ON market_snapshots_5m(market_slug)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_btc_prices_time ON btc_prices(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stats_strategy ON strategy_stats(strategy, variation)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_portfolios_name ON portfolios(name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_portfolio_trades_pid ON portfolio_trades(portfolio_id)")
        
        # Migrate existing database if needed
        self._migrate_database(cursor)
        
        self.conn.commit()
    
    def _migrate_database(self, cursor):
        """Migrate database schema if needed"""
        # Check if market_snapshots has new columns
        cursor.execute("PRAGMA table_info(market_snapshots)")
        columns = {row[1] for row in cursor.fetchall()}
        
        # If old schema, drop and recreate
        if 'up_bid' not in columns:
            print("Migrating database schema...")
            cursor.execute("DROP TABLE IF EXISTS market_snapshots")
            cursor.execute("""
                CREATE TABLE market_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    market_slug TEXT NOT NULL,
                    oracle_price REAL,
                    binance_price REAL,
                    up_bid REAL,
                    up_ask REAL,
                    up_mid REAL,
                    down_bid REAL,
                    down_ask REAL,
                    down_mid REAL,
                    time_to_expiry INTEGER,
                    metadata TEXT
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_time ON market_snapshots(timestamp)")
            print("✓ Database schema updated")
        
        # Check portfolios columns
        cursor.execute("PRAGMA table_info(portfolios)")
        p_cols = {row[1] for row in cursor.fetchall()}
        
        if 'strategy_name' not in p_cols and 'id' in p_cols:
            print("Migrating portfolios (adding strategy_name)...")
            cursor.execute("ALTER TABLE portfolios ADD COLUMN strategy_name TEXT DEFAULT 'Momentum Scalp'")
    
    # ==================== TRADES ====================
    
    def insert_trade(self, trade: Dict[str, Any]) -> int:
        """Insert a new trade"""
        cursor = self.conn.cursor()
        shares = trade.get('shares', trade.get('entry_shares', 0))
        cost = trade.get('cost', trade.get('entry_cost', 0))
        if cost == 0 and shares > 0 and trade.get('entry_price', 0) > 0:
            cost = shares * trade['entry_price']

        cursor.execute("""
            INSERT INTO trades (
                strategy, variation, market_slug, side,
                entry_time, entry_price, entry_shares, entry_cost,
                exit_time, exit_price, exit_reason, pnl, pnl_pct,
                balance_after, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            trade['strategy'],
            trade['variation'],
            trade['market_slug'],
            trade['side'],
            trade['entry_time'],
            trade['entry_price'],
            shares,
            cost,
            trade.get('exit_time'),
            trade.get('exit_price'),
            trade.get('exit_reason'),
            trade.get('pnl'),
            trade.get('pnl_pct'),
            trade.get('balance_after'),
            json.dumps(trade.get('metadata', {}))
        ))
        self.conn.commit()
        return cursor.lastrowid
    
    def update_trade_exit(self, trade_id: int, exit_data: Dict[str, Any]):
        """Update trade with exit information"""
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE trades SET
                exit_time = ?,
                exit_price = ?,
                exit_reason = ?,
                pnl = ?,
                pnl_pct = ?,
                balance_after = ?
            WHERE id = ?
        """, (
            exit_data['exit_time'],
            exit_data['exit_price'],
            exit_data['exit_reason'],
            exit_data['pnl'],
            exit_data['pnl_pct'],
            exit_data['balance_after'],
            trade_id
        ))
        self.conn.commit()
    
    def get_trades(self, strategy: Optional[str] = None, 
                   variation: Optional[str] = None,
                   limit: int = 100) -> List[Dict]:
        """Get trades with optional filters"""
        cursor = self.conn.cursor()
        
        query = "SELECT * FROM trades WHERE 1=1"
        params = []
        
        if strategy:
            query += " AND strategy = ?"
            params.append(strategy)
        
        if variation:
            query += " AND variation = ?"
            params.append(variation)
        
        query += " ORDER BY entry_time DESC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]
    
    # ==================== POSITIONS ====================
    
    def insert_position(self, position: Dict[str, Any]) -> int:
        """Insert a new position"""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO positions (
                strategy, variation, market_slug, side,
                entry_time, entry_price, shares, cost,
                current_price, unrealized_pnl, last_update, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            position['strategy'],
            position['variation'],
            position['market_slug'],
            position['side'],
            position['entry_time'],
            position['entry_price'],
            position['shares'],
            position['cost'],
            position.get('current_price'),
            position.get('unrealized_pnl'),
            datetime.now(timezone.utc).isoformat(),
            json.dumps(position.get('metadata', {}))
        ))
        self.conn.commit()
        return cursor.lastrowid
    
    def update_position(self, position_id: int, current_price: float, unrealized_pnl: float):
        """Update position with current price"""
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE positions SET
                current_price = ?,
                unrealized_pnl = ?,
                last_update = ?
            WHERE id = ?
        """, (
            current_price,
            unrealized_pnl,
            datetime.now(timezone.utc).isoformat(),
            position_id
        ))
        self.conn.commit()
    
    def delete_position(self, position_id: int):
        """Delete position (when closed)"""
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM positions WHERE id = ?", (position_id,))
        self.conn.commit()
    
    def delete_position_by_variation(self, strategy: str, variation: str):
        """Delete position by strategy and variation"""
        cursor = self.conn.cursor()
        cursor.execute(
            "DELETE FROM positions WHERE strategy = ? AND variation = ?", 
            (strategy, variation)
        )
        self.conn.commit()
    
    def get_all_positions(self) -> List[Dict]:
        """Get all open positions"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM positions")
        results = []
        for row in cursor.fetchall():
            d = dict(row)
            if d.get('metadata') and isinstance(d['metadata'], str):
                try:
                    d['metadata'] = json.loads(d['metadata'])
                except:
                    d['metadata'] = {}
            elif not d.get('metadata'):
                d['metadata'] = {}
            results.append(d)
        return results
        
    def get_positions(self, strategy: str, variation: str) -> List[Dict]:
        """Get all current positions"""
        cursor = self.conn.cursor()
        
        query = "SELECT * FROM positions WHERE 1=1"
        params = []
        
        if strategy:
            query += " AND strategy = ?"
            params.append(strategy)
        
        query += " ORDER BY entry_time DESC"
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]
    
    # ==================== MARKET SNAPSHOTS ====================
    
    def insert_market_snapshot(self, snapshot: Dict[str, Any]):
        """Insert 15m market price snapshot"""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO market_snapshots (
                timestamp, market_slug, oracle_price, binance_price,
                up_bid, up_ask, up_mid,
                down_bid, down_ask, down_mid,
                time_to_expiry, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            snapshot['timestamp'],
            snapshot['market_slug'],
            snapshot.get('oracle_price'),
            snapshot.get('binance_price'),
            snapshot.get('up_bid'),
            snapshot.get('up_ask'),
            snapshot.get('up_mid'),
            snapshot.get('down_bid'),
            snapshot.get('down_ask'),
            snapshot.get('down_mid'),
            snapshot.get('time_to_expiry'),
            json.dumps(snapshot.get('metadata', {}))
        ))
        self.conn.commit()

    def insert_market_snapshot_5m(self, snapshot: Dict[str, Any]):
        """Insert 5m market price snapshot"""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO market_snapshots_5m (
                timestamp, market_slug, oracle_price, binance_price,
                up_bid, up_ask, up_mid,
                down_bid, down_ask, down_mid,
                time_to_expiry, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            snapshot['timestamp'],
            snapshot['market_slug'],
            snapshot.get('oracle_price'),
            snapshot.get('binance_price'),
            snapshot.get('up_bid'),
            snapshot.get('up_ask'),
            snapshot.get('up_mid'),
            snapshot.get('down_bid'),
            snapshot.get('down_ask'),
            snapshot.get('down_mid'),
            snapshot.get('time_to_expiry'),
            json.dumps(snapshot.get('metadata', {}))
        ))
        self.conn.commit()

    def insert_btc_price(self, timestamp: str, binance_price: float,
                         oracle_price: float = None, lag_ms: int = None):
        """Insert BTC price tick (shared between 15m and 5m)"""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO btc_prices (timestamp, binance_price, oracle_price, lag_ms)
            VALUES (?, ?, ?, ?)
        """, (timestamp, binance_price, oracle_price, lag_ms))
        self.conn.commit()
    
    # ==================== STRATEGY STATS ====================
    
    def update_strategy_stats(self, stats: Dict[str, Any]):
        """Update strategy statistics"""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO strategy_stats (
                timestamp, strategy, variation, balance,
                total_trades, winning_trades, losing_trades,
                total_pnl, win_rate, avg_win, avg_loss,
                max_drawdown, sharpe_ratio, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.now(timezone.utc).isoformat(),
            stats['strategy'],
            stats['variation'],
            stats['balance'],
            stats.get('total_trades', 0),
            stats.get('winning_trades', 0),
            stats.get('losing_trades', 0),
            stats.get('total_pnl', 0),
            stats.get('win_rate', 0),
            stats.get('avg_win', 0),
            stats.get('avg_loss', 0),
            stats.get('max_drawdown', 0),
            stats.get('sharpe_ratio', 0),
            json.dumps(stats.get('metadata', {}))
        ))
        self.conn.commit()
    
    def get_latest_stats(self, strategy: Optional[str] = None) -> List[Dict]:
        """Get latest statistics for each variation"""
        cursor = self.conn.cursor()
        
        # Get latest stats for each strategy/variation combo
        query = """
            SELECT s1.* FROM strategy_stats s1
            INNER JOIN (
                SELECT strategy, variation, MAX(timestamp) as max_time
                FROM strategy_stats
                WHERE 1=1
        """
        params = []
        
        if strategy:
            query += " AND strategy = ?"
            params.append(strategy)
        
        query += """
                GROUP BY strategy, variation
            ) s2 ON s1.strategy = s2.strategy 
                AND s1.variation = s2.variation 
                AND s1.timestamp = s2.max_time
            ORDER BY s1.strategy, s1.variation
        """
        
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]
    
    # ==================== SYSTEM EVENTS ====================
    
    def log_event(self, event_type: str, message: str, 
                  severity: str = "INFO", metadata: Optional[Dict] = None):
        """Log system event"""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO system_events (timestamp, event_type, severity, message, metadata)
            VALUES (?, ?, ?, ?, ?)
        """, (
            datetime.now(timezone.utc).isoformat(),
            event_type,
            severity,
            message,
            json.dumps(metadata or {})
        ))
        self.conn.commit()
    
    # ==================== PORTFOLIOS ====================
    
    def insert_portfolio(self, portfolio: Dict[str, Any]) -> int:
        """Insert a new portfolio"""
        cursor = self.conn.cursor()
        now = datetime.now(timezone.utc).isoformat()
        cursor.execute("""
            INSERT INTO portfolios (
                name, strategy_name, type, initial_capital, current_balance,
                free_funds, min_bet, max_bet, bet_percentage, created_at, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            portfolio['name'],
            portfolio['strategy_name'],
            portfolio['type'],
            portfolio['initial_capital'],
            portfolio['initial_capital'], # current_balance starts at initial
            portfolio['initial_capital'], # free_funds starts at initial
            portfolio['min_bet'],
            portfolio['max_bet'],
            portfolio['bet_percentage'],
            now,
            json.dumps(portfolio.get('metadata', {}))
        ))
        self.conn.commit()
        return cursor.lastrowid
    
    def get_portfolios(self, strategy_filter: Optional[str] = None) -> List[Dict]:
        """Get all portfolios with optional strategy filter"""
        cursor = self.conn.cursor()
        if strategy_filter:
            cursor.execute("SELECT * FROM portfolios WHERE strategy_name = ?", (strategy_filter,))
        else:
            cursor.execute("SELECT * FROM portfolios")
        
        results = []
        for row in cursor.fetchall():
            d = dict(row)
            if d.get('metadata'):
                try:
                    d['metadata'] = json.loads(d['metadata'])
                except (json.JSONDecodeError, TypeError):
                    d['metadata'] = {}
            results.append(d)
        return results
    
    def get_portfolio_by_id(self, portfolio_id: int) -> Optional[Dict]:
        """Get portfolio by ID"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM portfolios WHERE id = ?", (portfolio_id,))
        row = cursor.fetchone()
        
        if not row:
            return None
            
        d = dict(row)
        if d.get('metadata'):
            try:
                d['metadata'] = json.loads(d['metadata'])
            except (json.JSONDecodeError, TypeError):
                d['metadata'] = {}
        return d
    
    def update_portfolio_settings(self, portfolio_id: int, settings: Dict[str, Any]):
        """Update portfolio settings (min_bet, max_bet, percentage)"""
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE portfolios SET
                min_bet = ?,
                max_bet = ?,
                bet_percentage = ?
            WHERE id = ?
        """, (
            settings['min_bet'],
            settings['max_bet'],
            settings['bet_percentage'],
            portfolio_id
        ))
        self.conn.commit()
    
    def update_portfolio_status(self, portfolio_id: int, status: str):
        """Update portfolio status (active/stopped)"""
        cursor = self.conn.cursor()
        cursor.execute("UPDATE portfolios SET status = ? WHERE id = ?", (status, portfolio_id))
        self.conn.commit()
    
    def add_withdrawal(self, portfolio_id: int, amount: float):
        """Add a withdrawal record and update portfolio balance"""
        cursor = self.conn.cursor()
        now = datetime.now(timezone.utc).isoformat()
        
        # Check if enough free funds
        cursor.execute("SELECT free_funds, current_balance FROM portfolios WHERE id = ?", (portfolio_id,))
        row = cursor.fetchone()
        if not row or row['free_funds'] < amount:
            raise ValueError("Insufficient free funds")
        
        # Insert withdrawal record
        cursor.execute("""
            INSERT INTO portfolio_withdrawals (portfolio_id, amount, timestamp)
            VALUES (?, ?, ?)
        """, (portfolio_id, amount, now))
        
        # Update portfolio
        cursor.execute("""
            UPDATE portfolios SET
                current_balance = current_balance - ?,
                free_funds = free_funds - ?
            WHERE id = ?
        """, (amount, amount, portfolio_id))
        
        self.conn.commit()

    def link_portfolio_trade(self, portfolio_id: int, trade_id: int):
        """Link a trade to a portfolio"""
        cursor = self.conn.cursor()
        cursor.execute("INSERT INTO portfolio_trades (portfolio_id, trade_id) VALUES (?, ?)", (portfolio_id, trade_id))
        self.conn.commit()

    def get_portfolio_trades(self, portfolio_id: int, limit: int = 200) -> List[Dict]:
        """Get trades for a specific portfolio"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT t.* FROM trades t
            JOIN portfolio_trades pt ON t.id = pt.trade_id
            WHERE pt.portfolio_id = ?
            ORDER BY t.entry_time DESC
            LIMIT ?
        """, (portfolio_id, limit))
        return [dict(row) for row in cursor.fetchall()]

    def get_portfolio_stats_summary(self) -> Dict:
        """Get summary statistics for all portfolios by type"""
        cursor = self.conn.cursor()
        
        # Overall counts
        cursor.execute("""
            SELECT type, COUNT(*) as count, 
                   AVG(CASE WHEN total_trades > 0 THEN winning_trades * 100.0 / total_trades ELSE 0 END) as avg_winrate,
                   AVG((current_balance - initial_capital) / initial_capital * 100.0) as avg_pnl_pct
            FROM (
                SELECT p.*, 
                       (SELECT COUNT(*) FROM portfolio_trades pt JOIN trades t ON pt.trade_id = t.id WHERE pt.portfolio_id = p.id AND t.pnl > 0) as winning_trades,
                       (SELECT COUNT(*) FROM portfolio_trades pt WHERE pt.portfolio_id = p.id) as total_trades
                FROM portfolios p
            )
            GROUP BY type
        """)
        type_stats = [dict(row) for row in cursor.fetchall()]
        
        cursor.execute("SELECT COUNT(*) as total FROM portfolios")
        total_count_row = cursor.fetchone()
        total_count = total_count_row['total'] if total_count_row else 0
        
        # Successful/Unsuccessful
        cursor.execute("SELECT COUNT(*) as count FROM portfolios WHERE current_balance > initial_capital")
        successful = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM portfolios WHERE current_balance < initial_capital")
        unsuccessful = cursor.fetchone()['count']
        
        # Best/Worst
        cursor.execute("SELECT name, (current_balance - initial_capital) as pnl_abs FROM portfolios ORDER BY pnl_abs DESC LIMIT 1")
        best_row = cursor.fetchone()
        best = dict(best_row) if best_row else None
        
        cursor.execute("SELECT name, (current_balance - initial_capital) as pnl_abs FROM portfolios ORDER BY pnl_abs ASC LIMIT 1")
        worst_row = cursor.fetchone()
        worst = dict(worst_row) if worst_row else None
        
        return {
            "type_stats": type_stats,
            "total_count": total_count,
            "successful_count": successful,
            "unsuccessful_count": unsuccessful,
            "best_portfolio": best,
            "worst_portfolio": worst
        }

        # Frozen funds table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS frozen_funds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portfolio_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                frozen_at TEXT NOT NULL,
                release_at TEXT NOT NULL,
                reason TEXT,
                FOREIGN KEY (portfolio_id) REFERENCES portfolios(id)
            )
        """)

    def calculate_portfolio_stats(self, portfolio_id: int) -> Dict[str, Any]:
        """Calculate portfolio stats from DB history"""
        p = self.get_portfolio_by_id(portfolio_id)
        if not p: return {}
        
        trades = self.get_portfolio_trades(portfolio_id, limit=10000)
        
        total_trades = len(trades)
        winning_trades = len([t for t in trades if t['pnl'] > 0])
        losing_trades = len([t for t in trades if t['pnl'] <= 0])
        total_pnl = sum(t['pnl'] for t in trades)
        
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
        avg_win = (sum(t['pnl'] for t in trades if t['pnl'] > 0) / winning_trades) if winning_trades > 0 else 0
        avg_loss = (sum(t['pnl'] for t in trades if t['pnl'] <= 0) / losing_trades) if losing_trades > 0 else 0
        
        # Balance from portfolio (authoritative)
        balance = p['current_balance'] 
        
        # Max Drawdown (simple calc from trade history, ideally implies sequence)
        # Reconstruct equity curve for drawdown
        equity = p['initial_capital']
        peak = equity
        max_dd = 0
        # Sort trades by exit time for correct sequence
        date_sorted = sorted(trades, key=lambda x: x['exit_time'] if x['exit_time'] else x['entry_time'])
        
        for t in date_sorted:
            equity += t['pnl']
            if equity > peak: peak = equity
            dd = (peak - equity) / peak * 100 if peak > 0 else 0
            if dd > max_dd: max_dd = dd

        return {
            "strategy": p['strategy_name'],
            "variation": p['name'],
            "balance": balance,
            "total_trades": total_trades,
            "winning_trades": winning_trades,
            "losing_trades": losing_trades,
            "total_pnl": total_pnl,
            "win_rate": round(win_rate, 2),
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2),
            "max_drawdown": round(max_dd, 2),
            "sharpe_ratio": 0 # Complex to calc correctly without risk free rate time series
        }
    def calculate_strategy_stats(self, variation_id: str, strategy_name: str, initial_balance: float = 100.0) -> Dict[str, Any]:
        """Calculate strategy stats from DB history (for Standard Strategies)"""
        # trades = self.get_trades_by_variation(variation_id) <--- REMOVED
        
        # Use explicit query
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM trades WHERE variation = ? ORDER BY entry_time ASC", (variation_id,))
        trades = [dict(row) for row in cursor.fetchall()]
        
        total_trades = len(trades)
        winning_trades = len([t for t in trades if t['pnl'] > 0])
        losing_trades = len([t for t in trades if t['pnl'] <= 0])
        total_pnl = sum(t['pnl'] for t in trades)
        
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
        avg_win = (sum(t['pnl'] for t in trades if t['pnl'] > 0) / winning_trades) if winning_trades > 0 else 0
        avg_loss = (sum(t['pnl'] for t in trades if t['pnl'] <= 0) / losing_trades) if losing_trades > 0 else 0
        
        # Current Balance (Simulated)
        balance = initial_balance + total_pnl
        
        # Max Drawdown
        equity = initial_balance
        peak = equity
        max_dd = 0
        
        date_sorted = sorted(trades, key=lambda x: x['exit_time'] if x['exit_time'] else x['entry_time'])
        
        for t in date_sorted:
            equity += t['pnl']
            if equity > peak: peak = equity
            dd = (peak - equity) / peak * 100 if peak > 0 else 0
            if dd > max_dd: max_dd = dd

        return {
            "strategy": strategy_name,
            "variation": variation_id,
            "balance": balance,
            "total_trades": total_trades,
            "winning_trades": winning_trades,
            "losing_trades": losing_trades,
            "total_pnl": total_pnl,
            "win_rate": round(win_rate, 2),
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2),
            "max_drawdown": round(max_dd, 2),
            "sharpe_ratio": 0,
            "roi": ((balance - initial_balance) / initial_balance * 100) if initial_balance > 0 else 0
        }

    def get_variations_with_trades(self) -> set:
        """Get set of variation IDs that have trades"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT DISTINCT variation FROM trades")
        rows = cursor.fetchall()
        return {row['variation'] for row in rows}

    def freeze_funds(self, portfolio_id: int, amount: float, duration_seconds: int, reason: str):
        """Freeze funds for a specific duration"""
        cursor = self.conn.cursor()
        now = datetime.now(timezone.utc)
        release_at = now + timedelta(seconds=duration_seconds)
        
        cursor.execute("""
            INSERT INTO frozen_funds (portfolio_id, amount, frozen_at, release_at, reason)
            VALUES (?, ?, ?, ?, ?)
        """, (
            portfolio_id, 
            amount, 
            now.isoformat(), 
            release_at.isoformat(), 
            reason
        ))
        
        # Note: Funds are NOT added to free_balance yet. They are just recorded as frozen.
        # When released, they will be added to free_balance.
        
        self.conn.commit()
    
    def get_releasable_funds(self) -> List[Dict]:
        """Get funds that are ready to be released"""
        cursor = self.conn.cursor()
        now = datetime.now(timezone.utc).isoformat()
        
        cursor.execute("SELECT * FROM frozen_funds WHERE release_at <= ?", (now,))
        return [dict(row) for row in cursor.fetchall()]

    def delete_frozen_fund(self, fund_id: int):
        """Delete frozen fund record (after release)"""
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM frozen_funds WHERE id = ?", (fund_id,))
        self.conn.commit()

    def update_portfolio_balance(self, portfolio_id: int, free_balance: float):
        """Update portfolio free balance"""
        cursor = self.conn.cursor()
        cursor.execute("UPDATE portfolios SET current_balance = ?, free_funds = ? WHERE id = ?", (free_balance, free_balance, portfolio_id))
        self.conn.commit()

    # ==================== UTILITY ====================
    
    def export_to_csv(self, table: str, output_path: str):
        """Export table to CSV"""
        import csv
        
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM {table}")
        
        rows = cursor.fetchall()
        if not rows:
            return
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([desc[0] for desc in cursor.description])
            writer.writerows(rows)
    
    def get_summary(self) -> Dict:
        """Get overall system summary"""
        cursor = self.conn.cursor()
        
        cursor.execute("SELECT COUNT(*) as count FROM trades")
        total_trades = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM positions")
        active_positions = cursor.fetchone()['count']
        
        cursor.execute("SELECT SUM(pnl) as total FROM trades WHERE pnl IS NOT NULL")
        total_pnl = cursor.fetchone()['total'] or 0
        
        cursor.execute("SELECT COUNT(DISTINCT strategy || variation) as count FROM strategy_stats")
        total_variations = cursor.fetchone()['count']
        
        return {
            "total_trades": total_trades,
            "active_positions": active_positions,
            "total_pnl": total_pnl,
            "total_variations": total_variations,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    def close(self):
        """Close database connection"""
        self.conn.close()


if __name__ == "__main__":
    # Test database creation
    db = TradingDatabase()
    
    print("Database created successfully!")
    print(f"Location: {db.db_path}")
    
    # Test inserting a sample trade
    trade = {
        "strategy": "momentum_scalp",
        "variation": "60s_0.50_B1",
        "market_slug": "btc-updown-15m-test",
        "side": "UP",
        "entry_time": datetime.now(timezone.utc).isoformat(),
        "entry_price": 0.45,
        "entry_shares": 2.22,
        "entry_cost": 1.00,
        "balance_after": 99.00
    }
    
    trade_id = db.insert_trade(trade)
    print(f"Inserted test trade with ID: {trade_id}")
    
    # Get summary
    summary = db.get_summary()
    print(f"\nDatabase summary: {summary}")
    
    db.close()
