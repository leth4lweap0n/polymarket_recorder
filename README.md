# Polymarket Recorder

**TR:** Polymarket prediction market verilerini 5-15 dakikalık aralıklarla kaydeden, backtest için kullanılabilecek dataset oluşturucu.

**EN:** A dataset recorder that captures Polymarket prediction market data at 5–15-minute intervals for backtesting purposes.

---

## Kurulum / Installation

```bash
pip install -r requirements.txt
```

Ortam değişkenlerini yapılandırmak için `.env.example` dosyasını kopyalayın:

```bash
cp .env.example .env
# .env dosyasını düzenleyin
```

---

## Kullanım / Usage

### Kayıt Başlatma / Start Recording

```bash
# Varsayılan ayarlarla başlat (5 dakika aralık, 100 market)
python main.py

# 10 dakika aralıkla kayıt
python main.py --interval 10

# Özel veritabanı yolu
python main.py --db-path custom.db

# Maksimum 50 market takip et
python main.py --max-markets 50
```

### Veri Export / Data Export

```bash
# CSV olarak dışa aktar
python main.py --export csv

# JSON olarak dışa aktar
python main.py --export json

# Özel export klasörü belirt
python main.py --export csv --export-dir my_exports
```

---

## Konfigürasyon / Configuration

Ayarlar `.env` dosyası veya ortam değişkenleri ile yapılandırılabilir.

| Değişken | Varsayılan | Açıklama |
|---|---|---|
| `RECORD_INTERVAL_MINUTES` | `5` | Kayıt aralığı (dakika) |
| `MAX_MARKETS` | `100` | Takip edilecek maks. market sayısı |
| `ORDERBOOK_DEPTH` | `10` | Order book derinliği (seviye) |
| `DB_PATH` | `data/polymarket_data.db` | SQLite veritabanı yolu |
| `CLOB_API_URL` | `https://clob.polymarket.com` | Polymarket CLOB API URL |
| `GAMMA_API_URL` | `https://gamma-api.polymarket.com` | Polymarket Gamma API URL |
| `API_RATE_LIMIT_SLEEP` | `0.5` | İstekler arası bekleme (saniye) |
| `API_CONNECT_TIMEOUT` | `10` | Bağlantı zaman aşımı (saniye) |
| `API_READ_TIMEOUT` | `30` | Okuma zaman aşımı (saniye) |
| `API_MAX_RETRIES` | `3` | Maksimum yeniden deneme sayısı |
| `LOG_LEVEL` | `INFO` | Log seviyesi (DEBUG/INFO/WARNING/ERROR) |
| `LOG_FILE` | `logs/polymarket_recorder.log` | Log dosya yolu |

---

## Veritabanı Şeması / Database Schema

### `markets`
| Sütun | Tür | Açıklama |
|---|---|---|
| `id` | TEXT PK | Polymarket condition_id |
| `question` | TEXT | Market sorusu |
| `description` | TEXT | Market açıklaması |
| `category` | TEXT | Kategori |
| `end_date` | TEXT | Bitiş tarihi |
| `active` | BOOLEAN | Aktif mi? |
| `created_at` | TIMESTAMP | Oluşturulma zamanı |
| `updated_at` | TIMESTAMP | Son güncelleme zamanı |

### `tokens`
| Sütun | Tür | Açıklama |
|---|---|---|
| `token_id` | TEXT PK | Outcome token ID |
| `market_id` | TEXT FK | İlişkili market |
| `outcome` | TEXT | "Yes" veya "No" |
| `created_at` | TIMESTAMP | Oluşturulma zamanı |

### `price_snapshots`
| Sütun | Tür | Açıklama |
|---|---|---|
| `id` | INTEGER PK | Otomatik artan ID |
| `token_id` | TEXT FK | Token referansı |
| `market_id` | TEXT FK | Market referansı |
| `price` | REAL | Güncel fiyat (0–1) |
| `bid_price` | REAL | En iyi alış fiyatı |
| `ask_price` | REAL | En iyi satış fiyatı |
| `spread` | REAL | Bid-ask spread |
| `timestamp` | TIMESTAMP | Snapshot zamanı |

### `orderbook_snapshots`
| Sütun | Tür | Açıklama |
|---|---|---|
| `id` | INTEGER PK | Otomatik artan ID |
| `token_id` | TEXT FK | Token referansı |
| `market_id` | TEXT FK | Market referansı |
| `side` | TEXT | "bid" veya "ask" |
| `level` | INTEGER | Derinlik seviyesi (1–10) |
| `price` | REAL | Seviye fiyatı |
| `size` | REAL | Seviye büyüklüğü |
| `timestamp` | TIMESTAMP | Snapshot zamanı |

### `volume_snapshots`
| Sütun | Tür | Açıklama |
|---|---|---|
| `id` | INTEGER PK | Otomatik artan ID |
| `market_id` | TEXT FK | Market referansı |
| `volume_24h` | REAL | 24 saatlik işlem hacmi |
| `liquidity` | REAL | Toplam likidite |
| `timestamp` | TIMESTAMP | Snapshot zamanı |

---

## Dosya Yapısı / Project Structure

```
polymarket_recorder/
├── README.md            # Bu dosya
├── requirements.txt     # Python bağımlılıkları
├── .env.example         # Örnek ortam değişkenleri
├── .gitignore           # Git yoksayma kuralları
├── config.py            # Konfigürasyon ayarları
├── models.py            # Veri modelleri (dataclass)
├── database.py          # SQLite veritabanı işlemleri
├── api_client.py        # Polymarket API istemcisi
├── recorder.py          # Ana kayıt döngüsü
├── utils.py             # Loglama, hata yönetimi, export
└── main.py              # Giriş noktası (CLI)
```