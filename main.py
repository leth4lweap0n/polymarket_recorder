"""
Giriş noktası - CLI argümanları işlenir ve uygun eylem başlatılır.
"""

import argparse
import sys

import config
from utils import export_to_csv, export_to_json, setup_logging


def parse_args() -> argparse.Namespace:
    """
    Komut satırı argümanlarını tanımlar ve ayrıştırır.

    Returns:
        Ayrıştırılmış argüman nesnesi.
    """
    parser = argparse.ArgumentParser(
        prog="polymarket_recorder",
        description="Polymarket prediction market verilerini kaydeden program.",
    )

    parser.add_argument(
        "--interval",
        type=int,
        default=config.RECORD_INTERVAL_MINUTES,
        metavar="DAKİKA",
        help=f"Kayıt aralığı (dakika, varsayılan: {config.RECORD_INTERVAL_MINUTES})",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=config.DB_PATH,
        metavar="YOL",
        help=f"SQLite veritabanı dosya yolu (varsayılan: {config.DB_PATH})",
    )
    parser.add_argument(
        "--max-markets",
        type=int,
        default=config.MAX_MARKETS,
        metavar="SAYI",
        help=f"Takip edilecek maksimum market sayısı (varsayılan: {config.MAX_MARKETS})",
    )
    parser.add_argument(
        "--export",
        choices=["csv", "json"],
        metavar="FORMAT",
        help="Veritabanını dışa aktar ve çık (csv veya json)",
    )
    parser.add_argument(
        "--export-dir",
        type=str,
        default="exports",
        metavar="KLASÖR",
        help="Export dosyalarının kaydedileceği klasör (varsayılan: exports)",
    )

    return parser.parse_args()


def main() -> None:
    """Ana fonksiyon: argümanları işler ve kaydedici döngüsünü ya export'u başlatır."""
    args = parse_args()

    # Loglama sistemini başlat
    setup_logging()

    # CLI argümanlarını config'e uygula (runtime override)
    config.RECORD_INTERVAL_MINUTES = args.interval
    config.DB_PATH = args.db_path
    config.MAX_MARKETS = args.max_markets

    if args.export:
        # Export modunda çalış: veritabanını dışa aktar ve çık
        if args.export == "csv":
            export_to_csv(args.db_path, output_dir=args.export_dir)
        else:
            export_to_json(args.db_path, output_dir=args.export_dir)
        sys.exit(0)

    # Kayıt döngüsünü başlat
    from recorder import Recorder

    recorder = Recorder(db_path=args.db_path)
    recorder.run()


if __name__ == "__main__":
    main()
