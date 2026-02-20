"""
Ana kayıt döngüsü - Marketleri keşfeder, verileri çeker ve veritabanına kaydeder.
"""

import logging
import signal
import time
from collections.abc import Callable
from datetime import datetime
from typing import List

import config
import database
from api_client import PolymarketAPIClient
from models import Market, OrderbookSnapshot, PriceSnapshot, Token, VolumeSnapshot

logger = logging.getLogger(__name__)


class Recorder:
    """
    Polymarket verilerini periyodik olarak kaydeden ana sınıf.
    SIGINT/SIGTERM sinyallerini yakalayarak düzgün kapanma sağlar.
    """

    def __init__(self, db_path: str = config.DB_PATH) -> None:
        self.db_path = db_path
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
    # Market keşfi ve kayıt
    # ------------------------------------------------------------------

    def discover_and_save_markets(self) -> List[Market]:
        """
        Gamma API'den aktif marketleri çeker, veritabanına kaydeder ve
        ilişkili token'ları da kaydeder.

        Returns:
            Keşfedilen Market nesnelerinin listesi.
        """
        logger.info("Market listesi güncelleniyor...")
        raw_markets = []
        offset = 0
        limit = 100

        # Sayfalama ile tüm aktif marketleri çek
        while len(raw_markets) < config.MAX_MARKETS:
            batch = self.client.get_markets(limit=limit, offset=offset)
            if not batch:
                break
            raw_markets.extend(batch)
            if len(batch) < limit:
                break
            offset += limit

        # MAX_MARKETS sınırını uygula
        raw_markets = raw_markets[: config.MAX_MARKETS]

        markets: List[Market] = []
        now = datetime.utcnow()

        for raw in raw_markets:
            market_id = raw.get("conditionId") or raw.get("condition_id") or raw.get("id", "")
            if not market_id:
                continue

            market = Market(
                id=market_id,
                question=raw.get("question", ""),
                description=raw.get("description", ""),
                category=raw.get("category", ""),
                end_date=raw.get("endDate") or raw.get("end_date", ""),
                active=bool(raw.get("active", True)),
                created_at=now,
                updated_at=now,
            )
            database.upsert_market(self.db_path, market)
            markets.append(market)

            # Token'ları kaydet
            for token_data in raw.get("tokens", []):
                token_id = token_data.get("token_id", "")
                outcome = token_data.get("outcome", "")
                if not token_id:
                    continue
                token = Token(
                    token_id=token_id,
                    market_id=market_id,
                    outcome=outcome,
                    created_at=now,
                )
                database.upsert_token(self.db_path, token)

        logger.info("%d market kaydedildi/güncellendi.", len(markets))
        return markets

    # ------------------------------------------------------------------
    # Snapshot alma
    # ------------------------------------------------------------------

    def record_snapshots_for_market(self, market: Market) -> None:
        """
        Belirtilen marketin token'ları için fiyat, order book ve hacim
        snapshot'larını alır ve veritabanına kaydeder.

        Args:
            market: Snapshot alınacak Market nesnesi.
        """
        tokens = database.get_tokens_for_market(self.db_path, market.id)
        if not tokens:
            logger.debug("Token bulunamadı, atlanıyor: %s", market.id)
            return

        now = datetime.utcnow()

        # Hacim verisi market bazında bir kez alınır
        volume_saved = False

        for token in tokens:
            # --- Fiyat ve spread ---
            price = self.client.get_price(token.token_id)
            spread_data = self.client.get_spread(token.token_id)

            bid_price: float | None = None
            ask_price: float | None = None
            spread: float | None = None

            if spread_data:
                bid_price = _safe_float(spread_data.get("bid"))
                ask_price = _safe_float(spread_data.get("ask"))
                spread = _safe_float(spread_data.get("spread"))

            if price is not None:
                ps = PriceSnapshot(
                    token_id=token.token_id,
                    market_id=market.id,
                    price=price,
                    bid_price=bid_price,
                    ask_price=ask_price,
                    spread=spread,
                    timestamp=now,
                )
                database.insert_price_snapshot(self.db_path, ps)

            # --- Order book ---
            orderbook = self.client.get_orderbook(token.token_id)
            if orderbook:
                ob_snapshots: List[OrderbookSnapshot] = []

                for side in ("bids", "asks"):
                    side_label = "bid" if side == "bids" else "ask"
                    entries = orderbook.get(side, [])
                    # En iyi ORDERBOOK_DEPTH seviyeyi kaydet
                    for level, entry in enumerate(entries[: config.ORDERBOOK_DEPTH], start=1):
                        p = _safe_float(entry.get("price"))
                        s = _safe_float(entry.get("size"))
                        if p is None or s is None:
                            continue
                        ob_snapshots.append(
                            OrderbookSnapshot(
                                token_id=token.token_id,
                                market_id=market.id,
                                side=side_label,
                                level=level,
                                price=p,
                                size=s,
                                timestamp=now,
                            )
                        )

                database.insert_orderbook_snapshots(self.db_path, ob_snapshots)

            # --- Hacim / Likidite (market bazında, ilk token'da kaydet) ---
            if not volume_saved:
                volume_24h = _safe_float(orderbook.get("volume") if orderbook else None) or 0.0
                liquidity = _safe_float(orderbook.get("liquidity") if orderbook else None) or 0.0
                vs = VolumeSnapshot(
                    market_id=market.id,
                    volume_24h=volume_24h,
                    liquidity=liquidity,
                    timestamp=now,
                )
                database.insert_volume_snapshot(self.db_path, vs)
                volume_saved = True

    # ------------------------------------------------------------------
    # Ana döngü
    # ------------------------------------------------------------------

    def run(self) -> None:
        """
        Periyodik kayıt döngüsünü başlatır.
        Her config.RECORD_INTERVAL_MINUTES dakikada bir snapshot alır.
        """
        logger.info(
            "Kayıt başlatılıyor — aralık: %d dk, maks market: %d, DB: %s",
            config.RECORD_INTERVAL_MINUTES,
            config.MAX_MARKETS,
            self.db_path,
        )

        # Veritabanını başlat
        database.initialize_database(self.db_path)
        self._running = True

        interval_seconds = config.RECORD_INTERVAL_MINUTES * 60

        try:
            while self._running:
                cycle_start = time.monotonic()

                # 1. Market listesini güncelle
                markets = self.discover_and_save_markets()

                # 2. Her aktif market için snapshot al
                snapshot_count = 0
                for market in markets:
                    if not self._running:
                        break
                    try:
                        self.record_snapshots_for_market(market)
                        snapshot_count += 1
                    except Exception as exc:
                        logger.error("Snapshot hatası (market=%s): %s", market.id, exc)

                elapsed = time.monotonic() - cycle_start
                logger.info(
                    "Döngü tamamlandı — %d market, %d snapshot, %.1fs",
                    len(markets),
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

def _safe_float(value) -> float | None:
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
