"""
Veri modelleri - Polymarket verileri için dataclass tanımları.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class Market:
    """Polymarket market bilgilerini temsil eder."""
    id: str                          # condition_id
    question: str
    description: str
    category: str
    end_date: str
    active: bool
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class Token:
    """Market outcome token'ını temsil eder (Yes/No)."""
    token_id: str
    market_id: str
    outcome: str                     # "Yes" veya "No"
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class PriceSnapshot:
    """Belirli bir andaki fiyat snapshot'ını temsil eder."""
    token_id: str
    market_id: str
    price: float                     # 0-1 arası olasılık
    bid_price: Optional[float]       # En iyi alış fiyatı
    ask_price: Optional[float]       # En iyi satış fiyatı
    spread: Optional[float]          # Bid-ask spread
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class OrderbookSnapshot:
    """Order book derinlik verisini temsil eder."""
    token_id: str
    market_id: str
    side: str                        # "bid" veya "ask"
    level: int                       # Seviye (1-10)
    price: float
    size: float
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class VolumeSnapshot:
    """Hacim ve likidite snapshot'ını temsil eder."""
    market_id: str
    volume_24h: float                # 24 saatlik hacim
    liquidity: float                 # Toplam likidite
    timestamp: datetime = field(default_factory=datetime.utcnow)
