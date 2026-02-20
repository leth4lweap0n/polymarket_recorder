"""
Yardımcı fonksiyonlar - Loglama kurulumu, hata yönetimi ve veri export işlemleri.
"""

import csv
import json
import logging
import os
import sqlite3
from logging.handlers import RotatingFileHandler
from typing import Any

import config


def setup_logging() -> logging.Logger:
    """
    Uygulama için hem konsola hem dosyaya yazan loglama yapılandırmasını kurar.
    Dosya için RotatingFileHandler kullanılır.

    Returns:
        Yapılandırılmış kök logger nesnesi.
    """
    log_dir = os.path.dirname(config.LOG_FILE)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    log_level = getattr(logging, config.LOG_LEVEL.upper(), logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Kök logger'ı yapılandır
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Önceki handler'ları temizle (tekrar çağrı durumuna karşı)
    root_logger.handlers.clear()

    # Konsol handler'ı
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Dönen dosya handler'ı
    file_handler = RotatingFileHandler(
        config.LOG_FILE,
        maxBytes=config.LOG_MAX_BYTES,
        backupCount=config.LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    return root_logger


# Veritabanındaki tablo adları sabit listesi (SQL injection'a karşı whitelist)
_ALLOWED_TABLES = frozenset(
    ["markets", "tokens", "price_snapshots", "orderbook_snapshots", "volume_snapshots"]
)


def _fetch_all_rows(db_path: str, table: str) -> list[dict[str, Any]]:
    """
    Belirtilen tabloyu sorgular ve sonuçları sözlük listesi olarak döndürür.
    Tablo adı sabit whitelist ile doğrulanır.

    Args:
        db_path: SQLite veritabanı dosya yolu.
        table:   Sorgulanacak tablo adı (whitelist'te olmalı).

    Returns:
        Her satırın sözlük olarak temsil edildiği liste.

    Raises:
        ValueError: Tablo adı izin verilenler arasında değilse.
    """
    if table not in _ALLOWED_TABLES:
        raise ValueError(f"İzin verilmeyen tablo adı: {table!r}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        # Tablo adı whitelist ile doğrulandı; SQLite parametre bağlama tablo
        # adlarını desteklemediğinden f-string kullanımı burada güvenlidir.
        cursor = conn.execute(f"SELECT * FROM {table}")  # noqa: S608
        rows = [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()
    return rows


def export_to_csv(db_path: str, output_dir: str = "exports") -> None:
    """
    SQLite veritabanındaki tüm tabloları CSV dosyalarına aktarır.
    Her tablo için ayrı bir CSV dosyası oluşturulur.

    Args:
        db_path:    SQLite veritabanı dosya yolu.
        output_dir: CSV dosyalarının kaydedileceği klasör.
    """
    logger = logging.getLogger(__name__)
    os.makedirs(output_dir, exist_ok=True)

    for table in sorted(_ALLOWED_TABLES):
        rows = _fetch_all_rows(db_path, table)
        if not rows:
            logger.info("Tablo boş, atlanıyor: %s", table)
            continue

        output_file = os.path.join(output_dir, f"{table}.csv")
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        logger.info("CSV export tamamlandı: %s (%d satır)", output_file, len(rows))


def export_to_json(db_path: str, output_dir: str = "exports") -> None:
    """
    SQLite veritabanındaki tüm tabloları JSON dosyalarına aktarır.
    Her tablo için ayrı bir JSON dosyası oluşturulur.

    Args:
        db_path:    SQLite veritabanı dosya yolu.
        output_dir: JSON dosyalarının kaydedileceği klasör.
    """
    logger = logging.getLogger(__name__)
    os.makedirs(output_dir, exist_ok=True)

    for table in sorted(_ALLOWED_TABLES):
        rows = _fetch_all_rows(db_path, table)
        output_file = os.path.join(output_dir, f"{table}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2, default=str)

        logger.info("JSON export tamamlandı: %s (%d kayıt)", output_file, len(rows))
