"""
Ana kayıt döngüsü - BTC up/down marketlerini keşfeder, verileri çeker ve JSON olarak kaydeder.
"""

import logging
import signal
import time
from collections.abc import Callable
from datetime import datetime
from typing import Any

import config
import storage
from api_client import PolymarketAPIClient

logger = logging.getLogger(__name__)


def _is_btc_market(question: str) -> bool:
    """
    Verilen market sorusunun BTC up/down marketi olup olmadığını kontrol eder.
    Soru hem BTC/Bitcoin hem de süre (5/10/15 dk) veya yön (up/down) anahtar
    kelimelerini içermelidir.

    Args:
        question: Market sorusu metni.

    Returns:
        BTC up/down marketi ise True, değilse False.
    """
    q = question.lower()
    # BTC veya Bitcoin geçmeli
    has_btc = any(kw in q for kw in config.BTC_KEYWORDS)
    # 5-15 dakika süresi veya up/down yönü belirtilmeli
    has_duration = any(kw in q for kw in config.DURATION_KEYWORDS)
    has_direction = "up" in q or "down" in q or "higher" in q or "lower" in q
    return has_btc and (has_duration or has_direction)


class Recorder:
    """
    Polymarket BTC up/down verilerini periyodik olarak JSON dosyalarına kaydeden sınıf.
    SIGINT/SIGTERM sinyallerini yakalayarak düzgün kapanma sağlar.
    """

    def __init__(self, data_dir: str = config.DATA_DIR) -> None:
        self.data_dir = data_dir
        self.client = PolymarketAPIClient()
        self._running = False

        # Sinyal handler'larını kaydet (graceful shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    # ------------------------------------------------------------------
    # Sinyal yönetimi
    # ------------------------------------------------------------------

    def _handle_shutdown(self, signum: int, frame) -> None:
        """SIGINT/SIGTERM sinyalinde döngüyü durdurur."""
        logger.info("Kapatma sinyali alındı (%s), döngü durduruluyor...", signum)
        self._running = False

    # ------------------------------------------------------------------
    # Market keşfi
    # ------------------------------------------------------------------

    def discover_btc_markets(self) -> list[dict[str, Any]]:
        """
        Gamma API'den tüm aktif marketleri çeker ve yalnızca BTC up/down
        marketlerini filtreler.

        Returns:
            BTC up/down market ham sözlüklerinin listesi.
        """
        logger.info("BTC market listesi güncelleniyor...")
        all_markets: list[dict] = []
        offset = 0
        limit = 100

        # Sayfalama ile aktif marketleri çek
        while True:
            batch = self.client.get_markets(limit=limit, offset=offset)
            if not batch:
                break
            all_markets.extend(batch)
            if len(batch) < limit:
                break
            offset += limit

        # BTC up/down filtresi uygula
        btc_markets = [m for m in all_markets if _is_btc_market(m.get("question", ""))]

        logger.info(
            "Toplam %d aktif market içinden %d BTC up/down market bulundu.",
            len(all_markets),
            len(btc_markets),
        )

        # Market meta verisini JSON olarak kaydet
        storage.save_markets(
            [
                {
                    "id": _get_market_id(m),
                    "question": m.get("question", ""),
                    "category": m.get("category", ""),
                    "end_date": m.get("endDate") or m.get("end_date", ""),
                    "active": m.get("active", True),
                }
                for m in btc_markets
            ],
            self.data_dir,
        )

        return btc_markets

    # ------------------------------------------------------------------
    # Snapshot alma
    # ------------------------------------------------------------------

    def record_snapshot_for_market(self, raw_market: dict[str, Any]) -> None:
        """
        Belirtilen BTC market için fiyat, order book ve hacim snapshot'ını alır,
        tek bir sözlük olarak paketler ve JSON Lines dosyasına ekler.

        Args:
            raw_market: Gamma API'den gelen ham market sözlüğü.
        """
        market_id = _get_market_id(raw_market)
        if not market_id:
            return

        now = datetime.utcnow()
        token_snapshots = []

        # Hacim ve likidite market genelinde tek kez tutulur
        volume_24h: float | None = None
        liquidity: float | None = None

        for token_data in raw_market.get("tokens", []):
            token_id = token_data.get("token_id", "")
            outcome = token_data.get("outcome", "")
            if not token_id:
                continue

            # --- Fiyat ---
            price = self.client.get_price(token_id)

            # --- Spread ---
            bid_price = ask_price = spread = None
            spread_data = self.client.get_spread(token_id)
            if spread_data:
                bid_price = _safe_float(spread_data.get("bid"))
                ask_price = _safe_float(spread_data.get("ask"))
                spread = _safe_float(spread_data.get("spread"))

            logger.debug(
                "Fiyat kaydedildi — market: %s | outcome: %s | price: %s | bid: %s | ask: %s | spread: %s",
                raw_market.get("question", market_id),
                outcome,
                price,
                bid_price,
                ask_price,
                spread,
            )

            # --- Order book ---
            orderbook_bids: list[dict] = []
            orderbook_asks: list[dict] = []

            ob = self.client.get_orderbook(token_id)
            if ob:
                for entry in ob.get("bids", [])[: config.ORDERBOOK_DEPTH]:
                    p, s = _safe_float(entry.get("price")), _safe_float(entry.get("size"))
                    if p is not None and s is not None:
                        orderbook_bids.append({"price": p, "size": s})

                for entry in ob.get("asks", [])[: config.ORDERBOOK_DEPTH]:
                    p, s = _safe_float(entry.get("price")), _safe_float(entry.get("size"))
                    if p is not None and s is not None:
                        orderbook_asks.append({"price": p, "size": s})

                # Hacim/likidite ilk geçerli orderbook yanıtından alınır
                if volume_24h is None:
                    volume_24h = _safe_float(ob.get("volume"))
                if liquidity is None:
                    liquidity = _safe_float(ob.get("liquidity"))

            token_snapshots.append({
                "token_id": token_id,
                "outcome": outcome,
                "price": price,
                "bid_price": bid_price,
                "ask_price": ask_price,
                "spread": spread,
                "orderbook_bids": orderbook_bids,
                "orderbook_asks": orderbook_asks,
            })

        # Tüm token bilgilerini market snapshot'ı altında topla
        snapshot = {
            "timestamp": now.isoformat(),
            "market_id": market_id,
            "question": raw_market.get("question", ""),
            "end_date": raw_market.get("endDate") or raw_market.get("end_date", ""),
            "volume_24h": volume_24h,
            "liquidity": liquidity,
            "tokens": token_snapshots,
        }

        storage.append_snapshot(snapshot, self.data_dir)

    # ------------------------------------------------------------------
    # Ana döngü
    # ------------------------------------------------------------------

    def run(self) -> None:
        """
        Periyodik kayıt döngüsünü başlatır.
        Her config.RECORD_INTERVAL_MINUTES dakikada bir BTC market snapshot'ı alır.
        """
        logger.info(
            "BTC kayıt başlatılıyor — aralık: %d dk, veri klasörü: %s",
            config.RECORD_INTERVAL_MINUTES,
            self.data_dir,
        )

        self._running = True
        interval_seconds = config.RECORD_INTERVAL_MINUTES * 60

        try:
            while self._running:
                cycle_start = time.monotonic()

                # 1. BTC marketleri güncelle
                btc_markets = self.discover_btc_markets()

                # 2. Her BTC market için snapshot al
                snapshot_count = 0
                for market in btc_markets:
                    if not self._running:
                        break
                    try:
                        self.record_snapshot_for_market(market)
                        snapshot_count += 1
                    except Exception as exc:
                        logger.error(
                            "Snapshot hatası (market=%s): %s",
                            market.get("question", "?"),
                            exc,
                        )

                elapsed = time.monotonic() - cycle_start
                logger.info(
                    "Döngü tamamlandı — %d BTC market, %d snapshot, %.1fs",
                    len(btc_markets),
                    snapshot_count,
                    elapsed,
                )

                # 3. Sonraki döngüye kadar bekle
                sleep_time = max(0.0, interval_seconds - elapsed)
                logger.debug("Sonraki döngüye %.1fs bekleniyor...", sleep_time)
                _interruptible_sleep(sleep_time, check=lambda: self._running)

        finally:
            self.client.close()
            logger.info("Kayıt durduruldu.")


# ------------------------------------------------------------------
# Yardımcı fonksiyonlar
# ------------------------------------------------------------------

def _get_market_id(market: dict[str, Any]) -> str:
    """
    Ham market sözlüğünden condition ID'yi çıkarır.
    Gamma API farklı sürümlerinde farklı alan adı kullanabilir.

    Args:
        market: Gamma API'den gelen ham market sözlüğü.

    Returns:
        Market condition ID veya boş string.
    """
    return market.get("conditionId") or market.get("condition_id") or market.get("id", "")


def _safe_float(value: Any) -> float | None:
    """Değeri float'a dönüştürür; başarısız olursa None döner."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _interruptible_sleep(seconds: float, check: Callable[[], bool], interval: float = 1.0) -> None:
    """
    Verilen süre boyunca uyur; her `interval` saniyede bir `check()` ile
    döngünün hâlâ çalışıp çalışmadığını kontrol eder.

    Args:
        seconds:  Toplam bekleme süresi (saniye).
        check:    Devam şartı; False döndürürse uyku kesilir.
        interval: Kontrol aralığı (saniye).
    """
    elapsed = 0.0
    while elapsed < seconds and check():
        time.sleep(min(interval, seconds - elapsed))
        elapsed += interval
