"""
Polymarket API istemcisi - CLOB ve Gamma API'lerinden veri çeker.
"""

import logging
import time
from typing import Any, Optional

import requests

import config

logger = logging.getLogger(__name__)


class PolymarketAPIClient:
    """
    Polymarket CLOB ve Gamma API'lerine erişim sağlayan istemci sınıfı.
    Rate limiting, retry ve session yönetimini kapsar.
    """

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def _request(self, url: str, params: Optional[dict] = None) -> Any:
        """
        HTTP GET isteği gönderir. Başarısız olursa exponential backoff ile
        en fazla config.API_MAX_RETRIES kez yeniden dener.

        Args:
            url:    İstek gönderilecek tam URL.
            params: Sorgu parametreleri (opsiyonel).

        Returns:
            JSON yanıt verisi (dict veya list).

        Raises:
            requests.HTTPError: Maksimum yeniden deneme sayısı aşıldığında.
        """
        last_exc: Exception = RuntimeError("İstek gönderilmedi")

        for attempt in range(1, config.API_MAX_RETRIES + 1):
            try:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=config.API_TIMEOUT,
                )
                response.raise_for_status()
                # Rate limit: istekler arası kısa bekleme
                time.sleep(config.API_RATE_LIMIT_SLEEP)
                return response.json()

            except requests.RequestException as exc:
                last_exc = exc
                wait = 2 ** attempt  # exponential backoff: 2s, 4s, 8s
                logger.warning(
                    "İstek başarısız (deneme %d/%d): %s — %ds bekleniyor",
                    attempt,
                    config.API_MAX_RETRIES,
                    exc,
                    wait,
                )
                time.sleep(wait)

        logger.error("Maksimum yeniden deneme aşıldı: %s", url)
        raise last_exc

    # ------------------------------------------------------------------
    # Gamma API metotları
    # ------------------------------------------------------------------

    def get_markets(self, limit: int = 100, offset: int = 0) -> list[dict]:
        """
        Gamma API'den aktif market listesini çeker.

        Args:
            limit:  Sayfa başına market sayısı.
            offset: Başlangıç offseti (sayfalama için).

        Returns:
            Market bilgilerini içeren sözlük listesi.
        """
        url = f"{config.GAMMA_API_URL}/markets"
        params = {
            "active": "true",
            "closed": "false",
            "limit": limit,
            "offset": offset,
        }
        data = self._request(url, params=params)
        # Gamma API bazen listeyi doğrudan, bazen "markets" anahtarıyla döner
        if isinstance(data, list):
            return data
        return data.get("markets", [])

    # ------------------------------------------------------------------
    # CLOB API metotları
    # ------------------------------------------------------------------

    def get_price(self, token_id: str) -> Optional[float]:
        """
        CLOB API'den belirtilen token için güncel mid-market fiyatını çeker.

        Args:
            token_id: Token ID (outcome token).

        Returns:
            0-1 arasında fiyat değeri; alınamazsa None.
        """
        url = f"{config.CLOB_API_URL}/midpoint"
        params = {"token_id": token_id}
        try:
            data = self._request(url, params=params)
            mid = data.get("mid")
            return float(mid) if mid is not None else None
        except Exception as exc:
            logger.warning("Fiyat alınamadı (token=%s): %s", token_id, exc)
            return None

    def get_orderbook(self, token_id: str) -> Optional[dict]:
        """
        CLOB API'den belirtilen token için order book verisini çeker.

        Args:
            token_id: Token ID (outcome token).

        Returns:
            Bid/ask listelerini içeren sözlük; alınamazsa None.
        """
        url = f"{config.CLOB_API_URL}/book"
        params = {"token_id": token_id}
        try:
            return self._request(url, params=params)
        except Exception as exc:
            logger.warning("Order book alınamadı (token=%s): %s", token_id, exc)
            return None

    def get_spread(self, token_id: str) -> Optional[dict]:
        """
        CLOB API'den belirtilen token için spread verisini (bid/ask) çeker.

        Args:
            token_id: Token ID (outcome token).

        Returns:
            "bid", "ask", "spread" anahtarlarını içeren sözlük; alınamazsa None.
        """
        url = f"{config.CLOB_API_URL}/spread"
        params = {"token_id": token_id}
        try:
            return self._request(url, params=params)
        except Exception as exc:
            logger.warning("Spread alınamadı (token=%s): %s", token_id, exc)
            return None

    def get_trades(self, token_id: str, after_id: Optional[str] = None) -> list[dict]:
        """
        CLOB API'den belirtilen token için son işlem (tick) listesini çeker.
        `after_id` verilirse yalnızca o ID'den sonraki işlemler döner;
        verilmezse en yeni `config.TICK_FETCH_LIMIT` kadar işlem döner.

        CLOB API yanıt formatı:
          - Eski sürüm: doğrudan list döner.
          - Yeni sürüm: {"data": [...], "next_cursor": "..."} şeklinde döner.
        Her iki format da desteklenir.

        Args:
            token_id: Token ID (outcome token).
            after_id: Son görülen işlem ID'si — yalnızca bundan yeni olanlar çekilir.

        Returns:
            İşlem (tick) sözlüklerinden oluşan liste; hata durumunda boş liste.
        """
        url = f"{config.CLOB_API_URL}/trades"
        params: dict = {"token_id": token_id, "limit": config.TICK_FETCH_LIMIT}
        if after_id:
            params["after"] = after_id
        try:
            data = self._request(url, params=params)
            if isinstance(data, list):
                return data
            return data.get("data", [])
        except Exception as exc:
            logger.warning("Tick verisi alınamadı (token=%s): %s", token_id, exc)
            return []

    def close(self) -> None:
        """HTTP session'ı kapatır."""
        self.session.close()
