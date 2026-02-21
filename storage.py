"""
JSON depolama modülü - BTC market, snapshot ve tick verilerini JSON dosyalarına kaydeder.

Dosya yapısı:
    data/
        btc_markets.json          — takip edilen BTC marketlerinin meta bilgisi
        snapshots/
            YYYY-MM-DD.jsonl      — günlük periyodik snapshot dosyaları (JSON Lines)
        ticks/
            YYYY-MM-DD.jsonl      — günlük tick (bireysel işlem) dosyaları (JSON Lines)
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


def _ensure_dirs(data_dir: str) -> None:
    """Gerekli klasörleri oluşturur."""
    os.makedirs(os.path.join(data_dir, "snapshots"), exist_ok=True)
    os.makedirs(os.path.join(data_dir, "ticks"), exist_ok=True)


def save_markets(markets: list[dict[str, Any]], data_dir: str) -> None:
    """
    Takip edilen BTC market listesini JSON dosyasına kaydeder.
    Dosya her çağrıda tamamen üzerine yazılır.

    Args:
        markets:  Kaydedilecek market sözlüklerinin listesi.
        data_dir: Veri klasörü kök yolu.
    """
    _ensure_dirs(data_dir)
    path = os.path.join(data_dir, "btc_markets.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(markets, f, ensure_ascii=False, indent=2, default=str)
    logger.debug("Market listesi kaydedildi: %s (%d market)", path, len(markets))


def append_snapshot(snapshot: dict[str, Any], data_dir: str) -> None:
    """
    Tek bir market snapshot'ını günlük JSON Lines dosyasına ekler.
    Dosya adı UTC tarihe göre belirlenir: YYYY-MM-DD.jsonl

    Args:
        snapshot: Kaydedilecek snapshot sözlüğü.
        data_dir: Veri klasörü kök yolu.
    """
    _ensure_dirs(data_dir)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = os.path.join(data_dir, "snapshots", f"{today}.jsonl")
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(snapshot, ensure_ascii=False, default=str) + "\n")


def append_ticks(ticks: list[dict[str, Any]], data_dir: str) -> None:
    """
    Tick (bireysel işlem) listesini günlük JSON Lines dosyasına ekler.
    Her tick ayrı bir satır olarak yazılır.
    Dosya adı UTC tarihe göre belirlenir: YYYY-MM-DD.jsonl

    Args:
        ticks:    Kaydedilecek tick sözlüklerinin listesi.
        data_dir: Veri klasörü kök yolu.
    """
    if not ticks:
        return
    _ensure_dirs(data_dir)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = os.path.join(data_dir, "ticks", f"{today}.jsonl")
    with open(path, "a", encoding="utf-8") as f:
        for tick in ticks:
            f.write(json.dumps(tick, ensure_ascii=False, default=str) + "\n")
    logger.debug("Tick kaydedildi: %s (%d işlem)", path, len(ticks))
