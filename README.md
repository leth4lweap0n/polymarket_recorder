# Polymarket BTC Recorder

**TR:** Polymarket üzerindeki 5–15 dakikalık BTC up/down prediction market verilerini periyodik olarak JSON dosyalarına kaydeden backtest dataset oluşturucu.

**EN:** A dataset recorder that captures Polymarket's 5–15-minute BTC up/down prediction market snapshots into JSON files for backtesting.

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

```bash
# Varsayılan ayarlarla başlat (5 dakika aralık)
python main.py

# 10 dakika aralıkla kayıt
python main.py --interval 10

# Özel veri klasörü
python main.py --data-dir my_data
```

---

## Konfigürasyon / Configuration

Ayarlar `.env` dosyası veya ortam değişkenleri ile yapılandırılabilir.

| Değişken | Varsayılan | Açıklama |
|---|---|---|
| `RECORD_INTERVAL_MINUTES` | `5` | Kayıt aralığı (dakika) |
| `ORDERBOOK_DEPTH` | `10` | Order book derinliği (seviye) |
| `TICK_FETCH_LIMIT` | `100` | Döngü başına çekilecek maks. tick sayısı |
| `DATA_DIR` | `data` | JSON snapshot/tick dosyaları klasörü |
| `CLOB_API_URL` | `https://clob.polymarket.com` | Polymarket CLOB API URL |
| `GAMMA_API_URL` | `https://gamma-api.polymarket.com` | Polymarket Gamma API URL |
| `API_RATE_LIMIT_SLEEP` | `0.5` | İstekler arası bekleme (saniye) |
| `API_CONNECT_TIMEOUT` | `10` | Bağlantı zaman aşımı (saniye) |
| `API_READ_TIMEOUT` | `30` | Okuma zaman aşımı (saniye) |
| `API_MAX_RETRIES` | `3` | Maksimum yeniden deneme sayısı |
| `LOG_LEVEL` | `INFO` | Log seviyesi (DEBUG/INFO/WARNING/ERROR) |
| `LOG_FILE` | `logs/polymarket_recorder.log` | Log dosya yolu |

---

## Market Filtresi / Market Filter

Yalnızca sorusunda **BTC veya Bitcoin** ile birlikte aşağıdakilerden birini içeren marketler takip edilir:

- Süre belirteci: `5 min`, `10 min`, `15 min` gibi kısa vadeliler
- Yön belirteci: `up`, `down`, `higher`, `lower`

Örnek: *"Will BTC be higher in 5 minutes?"*, *"BTC up or down in 15 min?"*

---

## Çıktı Yapısı / Output Structure

```
data/
├── btc_markets.json          # Anlık BTC market meta verisi (her döngüde güncellenir)
├── snapshots/
│   ├── 2024-01-01.jsonl      # Günlük periyodik snapshot dosyası (JSON Lines)
│   └── ...
└── ticks/
    ├── 2024-01-01.jsonl      # Günlük tick (bireysel işlem) dosyası (JSON Lines)
    └── ...
```

### Snapshot formatı (her döngüde bir satır / one line per cycle)

```json
{
  "timestamp": "2024-01-01T12:00:00",
  "market_id": "0xabc...",
  "question": "Will BTC be higher in 5 minutes?",
  "end_date": "2024-01-01T12:05:00",
  "volume_24h": 50000.0,
  "liquidity": 10000.0,
  "tokens": [
    {
      "token_id": "0x123...",
      "outcome": "Yes",
      "price": 0.65,
      "bid_price": 0.64,
      "ask_price": 0.66,
      "spread": 0.02,
      "orderbook_bids": [{"price": 0.64, "size": 100.0}],
      "orderbook_asks": [{"price": 0.66, "size": 80.0}]
    }
  ]
}
```

### Tick formatı (her işlem için bir satır / one line per trade)

```json
{
  "trade_id": "abc123",
  "token_id": "0x123...",
  "market_id": "0xabc...",
  "outcome": "Yes",
  "price": 0.65,
  "size": 150.0,
  "side": "BUY",
  "timestamp": "2024-01-01T12:00:01"
}
```

### Backtest için okuma / Reading for backtesting

```python
import json

# Periyodik snapshot'lar
with open("data/snapshots/2024-01-01.jsonl") as f:
    snapshots = [json.loads(line) for line in f]

# Tick verisi (bireysel işlemler)
with open("data/ticks/2024-01-01.jsonl") as f:
    ticks = [json.loads(line) for line in f]
```

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
├── storage.py           # JSON dosya kayıt işlemleri
├── api_client.py        # Polymarket API istemcisi
├── recorder.py          # Ana kayıt döngüsü (BTC filtreli)
├── utils.py             # Loglama kurulumu
└── main.py              # Giriş noktası (CLI)
```