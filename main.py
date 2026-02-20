"""
Giriş noktası - CLI argümanları işlenir ve kayıt döngüsü başlatılır.
"""

import argparse

import config
from utils import setup_logging


def parse_args() -> argparse.Namespace:
    """
    Komut satırı argümanlarını tanımlar ve ayrıştırır.

    Returns:
        Ayrıştırılmış argüman nesnesi.
    """
    parser = argparse.ArgumentParser(
        prog="polymarket_recorder",
        description="Polymarket BTC up/down market verilerini JSON olarak kaydeden program.",
    )

    parser.add_argument(
        "--interval",
        type=int,
        default=config.RECORD_INTERVAL_MINUTES,
        metavar="DAKİKA",
        help=f"Kayıt aralığı (dakika, varsayılan: {config.RECORD_INTERVAL_MINUTES})",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=config.DATA_DIR,
        metavar="KLASÖR",
        help=f"JSON snapshot dosyalarının kaydedileceği klasör (varsayılan: {config.DATA_DIR})",
    )

    return parser.parse_args()


def main() -> None:
    """Ana fonksiyon: argümanları işler ve BTC kayıt döngüsünü başlatır."""
    args = parse_args()

    # Loglama sistemini başlat
    setup_logging()

    # CLI argümanlarını config'e uygula (runtime override)
    config.RECORD_INTERVAL_MINUTES = args.interval
    config.DATA_DIR = args.data_dir

    # Kayıt döngüsünü başlat
    from recorder import Recorder

    recorder = Recorder(data_dir=args.data_dir)
    recorder.run()


if __name__ == "__main__":
    main()
