"""
Veritabanı işlemleri - SQLite tabloları oluşturma ve veri kaydetme.
"""

import logging
import os
import sqlite3
from datetime import datetime
from typing import List

from models import Market, OrderbookSnapshot, PriceSnapshot, Token, VolumeSnapshot

logger = logging.getLogger(__name__)


def get_connection(db_path: str) -> sqlite3.Connection:
    """
    Veritabanı bağlantısı oluşturur ve foreign key desteğini etkinleştirir.

    Args:
        db_path: SQLite veritabanı dosya yolu.

    Returns:
        Açık SQLite bağlantı nesnesi.
    """
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def initialize_database(db_path: str) -> None:
    """
    Gerekli tabloları oluşturur (yoksa). Uygulama başlangıcında çağrılmalıdır.

    Args:
        db_path: SQLite veritabanı dosya yolu.
    """
    conn = get_connection(db_path)
    try:
        cursor = conn.cursor()

        # markets tablosu
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS markets (
                id          TEXT PRIMARY KEY,
                question    TEXT,
                description TEXT,
                category    TEXT,
                end_date    TEXT,
                active      BOOLEAN,
                created_at  TIMESTAMP,
                updated_at  TIMESTAMP
            )
        """)

        # tokens tablosu
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tokens (
                token_id   TEXT PRIMARY KEY,
                market_id  TEXT REFERENCES markets(id),
                outcome    TEXT,
                created_at TIMESTAMP
            )
        """)

        # price_snapshots tablosu
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS price_snapshots (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                token_id  TEXT REFERENCES tokens(token_id),
                market_id TEXT REFERENCES markets(id),
                price     REAL,
                bid_price REAL,
                ask_price REAL,
                spread    REAL,
                timestamp TIMESTAMP
            )
        """)

        # orderbook_snapshots tablosu
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS orderbook_snapshots (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                token_id  TEXT REFERENCES tokens(token_id),
                market_id TEXT REFERENCES markets(id),
                side      TEXT,
                level     INTEGER,
                price     REAL,
                size      REAL,
                timestamp TIMESTAMP
            )
        """)

        # volume_snapshots tablosu
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS volume_snapshots (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id   TEXT REFERENCES markets(id),
                volume_24h  REAL,
                liquidity   REAL,
                timestamp   TIMESTAMP
            )
        """)

        conn.commit()
        logger.info("Veritabanı başarıyla başlatıldı: %s", db_path)
    finally:
        conn.close()


def upsert_market(db_path: str, market: Market) -> None:
    """
    Marketi ekler; zaten varsa aktiflik durumunu ve güncelleme zamanını günceller.

    Args:
        db_path: SQLite veritabanı dosya yolu.
        market:  Kaydedilecek Market nesnesi.
    """
    conn = get_connection(db_path)
    try:
        conn.execute(
            """
            INSERT INTO markets (id, question, description, category, end_date, active, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                active     = excluded.active,
                updated_at = excluded.updated_at
            """,
            (
                market.id,
                market.question,
                market.description,
                market.category,
                market.end_date,
                market.active,
                market.created_at.isoformat(),
                market.updated_at.isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def upsert_token(db_path: str, token: Token) -> None:
    """
    Token'ı ekler; zaten varsa yoksayar.

    Args:
        db_path: SQLite veritabanı dosya yolu.
        token:   Kaydedilecek Token nesnesi.
    """
    conn = get_connection(db_path)
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO tokens (token_id, market_id, outcome, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (
                token.token_id,
                token.market_id,
                token.outcome,
                token.created_at.isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def insert_price_snapshot(db_path: str, snapshot: PriceSnapshot) -> None:
    """
    Fiyat snapshot'ını veritabanına ekler.

    Args:
        db_path:  SQLite veritabanı dosya yolu.
        snapshot: Kaydedilecek PriceSnapshot nesnesi.
    """
    conn = get_connection(db_path)
    try:
        conn.execute(
            """
            INSERT INTO price_snapshots (token_id, market_id, price, bid_price, ask_price, spread, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot.token_id,
                snapshot.market_id,
                snapshot.price,
                snapshot.bid_price,
                snapshot.ask_price,
                snapshot.spread,
                snapshot.timestamp.isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def insert_orderbook_snapshots(db_path: str, snapshots: List[OrderbookSnapshot]) -> None:
    """
    Birden fazla order book snapshot'ını tek bir transaction ile ekler.

    Args:
        db_path:   SQLite veritabanı dosya yolu.
        snapshots: Kaydedilecek OrderbookSnapshot listesi.
    """
    if not snapshots:
        return

    conn = get_connection(db_path)
    try:
        conn.executemany(
            """
            INSERT INTO orderbook_snapshots (token_id, market_id, side, level, price, size, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    s.token_id,
                    s.market_id,
                    s.side,
                    s.level,
                    s.price,
                    s.size,
                    s.timestamp.isoformat(),
                )
                for s in snapshots
            ],
        )
        conn.commit()
    finally:
        conn.close()


def insert_volume_snapshot(db_path: str, snapshot: VolumeSnapshot) -> None:
    """
    Hacim ve likidite snapshot'ını veritabanına ekler.

    Args:
        db_path:  SQLite veritabanı dosya yolu.
        snapshot: Kaydedilecek VolumeSnapshot nesnesi.
    """
    conn = get_connection(db_path)
    try:
        conn.execute(
            """
            INSERT INTO volume_snapshots (market_id, volume_24h, liquidity, timestamp)
            VALUES (?, ?, ?, ?)
            """,
            (
                snapshot.market_id,
                snapshot.volume_24h,
                snapshot.liquidity,
                snapshot.timestamp.isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_active_markets(db_path: str) -> List[str]:
    """
    Veritabanındaki aktif market ID'lerini döndürür.

    Args:
        db_path: SQLite veritabanı dosya yolu.

    Returns:
        Aktif market ID'lerinden oluşan liste.
    """
    conn = get_connection(db_path)
    try:
        cursor = conn.execute("SELECT id FROM markets WHERE active = 1")
        return [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()


def get_tokens_for_market(db_path: str, market_id: str) -> List[Token]:
    """
    Belirtilen markete ait token'ları döndürür.

    Args:
        db_path:   SQLite veritabanı dosya yolu.
        market_id: Market ID (condition_id).

    Returns:
        Token nesnelerinden oluşan liste.
    """
    conn = get_connection(db_path)
    try:
        cursor = conn.execute(
            "SELECT token_id, market_id, outcome, created_at FROM tokens WHERE market_id = ?",
            (market_id,),
        )
        return [
            Token(
                token_id=row[0],
                market_id=row[1],
                outcome=row[2],
                created_at=datetime.fromisoformat(row[3]),
            )
            for row in cursor.fetchall()
        ]
    finally:
        conn.close()
