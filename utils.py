"""
Yardımcı fonksiyonlar - Loglama kurulumu.
"""

import logging
import os
from logging.handlers import RotatingFileHandler

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
