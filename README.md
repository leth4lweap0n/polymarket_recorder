# Polymarket BTC 15m & 5m Data Recorder

Continuous real-time data recorder for Bitcoin prices (Binance) and Polymarket prediction market data. Designed for high-frequency data collection to support backtesting and strategy analysis.

## Features

- **Multi-Market Recording**: Simultaneously captures data for both **15-minute** and **5-minute** BTC price markets.
- **Multi-Source Data**: Records Binance spot prices, Polymarket Orderbook (CLOB), Oracle prices (RTDS), and Target prices.
- **Asynchronous & Non-Blocking**: Uses `asyncio` for network operations and a dedicated background `DBWriter` thread for SQLite operations to ensure zero data loss during high volatility.
- **Security First**: Sensitive data (proxies) is managed via environment variables and `.env` files.
- **Daily Rotation**: Automatically creates a new SQLite database every day (`db/recorder_YYYY-MM-DD.db`) for easy data management.
- **Health Monitoring**: Built-in connection watchdog and heartbeat system to monitor data feed stability.
- **Real-time Console UI**: Live status line showing current prices, latency (lag), and connection status.

---

## Installation Guide

### Prerequisites
- **Python 3.10 or higher**
- **Git**

### 🪟 Windows Installation
1. **Clone the repository**:
   ```bash
   git clone https://github.com/MixasV/Polymarket-recorder.git
   cd Polymarket-recorder
   ```
2. **Create a virtual environment**:
   ```bash
   python -m venv venv
   ```
3. **Activate the environment**:
   ```bash
   .\venv\Scripts\activate
   ```
4. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
5. **Configure environment**:
   - Copy `.env.example` to `.env`
   - Edit `.env` and set your proxy credentials or set `USE_PROXY=False`.

### 🐧 Linux Installation (Ubuntu/Debian/CentOS)
1. **Clone the repository**:
   ```bash
   git clone https://github.com/MixasV/Polymarket-recorder.git
   cd Polymarket-recorder
   ```
2. **Install Python venv package** (if missing):
   ```bash
   sudo apt update
   sudo apt install python3-venv -y
   ```
3. **Create a virtual environment**:
   ```bash
   python3 -m venv venv
   ```
4. **Activate the environment**:
   ```bash
   source venv/bin/activate
   ```
5. **Install dependencies**:
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```
6. **Configure environment**:
   - `cp .env.example .env`
   - Edit `.env` to set `USE_PROXY` and proxy details.

---

## Usage

### Running the Recorder
1. Ensure your environment is activated.
2. Start the script:
   ```bash
   python data_recorder.py
   ```
3. **Configuration Loading Priority**:
   - The script first checks the `.env` file.
   - If `.env` is missing, it looks for System Environment Variables.
   - If running in an interactive terminal, it will ask for proxy preference.

### Console Indicators
- `BNC`: Latest Binance BTC price.
- `ORC`: Latest Polymarket Oracle price.
- `LAG`: Latency (ms) calculated by matching Oracle price to Binance price history.
- `15m/5m`: Currently active Polymarket market slugs.
- `U/D`: Best Bid/Ask for UP and DOWN tokens.

---

## Autostart & Reliability

### 🐧 Linux (systemd) - Recommended
To ensure the recorder starts on boot and restarts automatically:
1. Update paths in `recorder.service`.
2. Copy and enable:
   ```bash
   sudo cp recorder.service /etc/systemd/system/recorder.service
   sudo systemctl daemon-reload
   sudo systemctl enable recorder
   sudo systemctl start recorder
   ```

---

# Регистратор данных Polymarket BTC 15м и 5м (RU)

Скрипт для непрерывной записи цен Bitcoin (Binance) и данных рынков Polymarket (15-минутные и 5-минутные интервалы) в режиме реального времени.

## Основные изменения
- **Поддержка .env**: Все конфиденциальные данные (прокси) теперь вынесены в файл `.env`. Не забудьте создать его из `.env.example`.
- **Два типа рынков**: Скрипт одновременно записывает данные для рынков с интервалом 15 и 5 минут.
- **Улучшенная стабильность**: По умолчанию прокси выключен (`USE_PROXY=False`), чтобы избежать ошибок при первом запуске на сервере.
