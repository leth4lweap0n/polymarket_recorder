"""
Konfigürasyon ayarları - Ortam değişkenleri ve varsayılan değerler.
"""

import os
from dotenv import load_dotenv

# .env dosyasını yükle (varsa)
load_dotenv()

# --- Kayıt Ayarları ---
# Kayıt aralığı (dakika); 5-15 arası ayarlanabilir
RECORD_INTERVAL_MINUTES: int = int(os.getenv("RECORD_INTERVAL_MINUTES", "5"))

# Takip edilecek maksimum market sayısı
MAX_MARKETS: int = int(os.getenv("MAX_MARKETS", "100"))

# Order book derinliği (kaç seviye kaydedilecek)
ORDERBOOK_DEPTH: int = int(os.getenv("ORDERBOOK_DEPTH", "10"))

# --- Veritabanı Ayarları ---
# SQLite veritabanı dosya yolu
DB_PATH: str = os.getenv("DB_PATH", "data/polymarket_data.db")

# --- API Ayarları ---
# Polymarket CLOB API temel URL'i
CLOB_API_URL: str = os.getenv("CLOB_API_URL", "https://clob.polymarket.com")

# Polymarket Gamma API temel URL'i (market listesi için)
GAMMA_API_URL: str = os.getenv("GAMMA_API_URL", "https://gamma-api.polymarket.com")

# İstekler arası bekleme süresi (saniye) - rate limiting için
API_RATE_LIMIT_SLEEP: float = float(os.getenv("API_RATE_LIMIT_SLEEP", "0.5"))

# İstek zaman aşımı (saniye): (bağlantı, okuma)
API_TIMEOUT: tuple = (
    int(os.getenv("API_CONNECT_TIMEOUT", "10")),
    int(os.getenv("API_READ_TIMEOUT", "30")),
)

# Başarısız isteklerde maksimum yeniden deneme sayısı
API_MAX_RETRIES: int = int(os.getenv("API_MAX_RETRIES", "3"))

# --- Log Ayarları ---
# Log seviyesi: DEBUG, INFO, WARNING, ERROR
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

# Log dosya yolu
LOG_FILE: str = os.getenv("LOG_FILE", "logs/polymarket_recorder.log")

# Log dosyası maksimum boyutu (byte) - rotating handler için
LOG_MAX_BYTES: int = int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024)))  # 10 MB

# Tutulacak yedek log dosyası sayısı
LOG_BACKUP_COUNT: int = int(os.getenv("LOG_BACKUP_COUNT", "5"))
