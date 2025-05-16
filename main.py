import argparse
from src.candle_data_downloader import CandleDataDownloader
from src.config import Config

def parse_arguments():
    parser = argparse.ArgumentParser(description="Download OHLCV candle data from cryptocurrency exchanges")
    parser.add_argument("--config", "-c", type=str, default="config.cfg", help="Path to config file")
    parser.add_argument("--pairs", "-p", type=str, help="Comma-separated list of trading pairs (overrides config)")
    parser.add_argument("--timeframes", "-t", type=str, help="Comma-separated list of timeframes (overrides config)")
    parser.add_argument("--most-traded", "-m", action="store_true", help="Download most traded pairs")
    parser.add_argument("--days", "-d", type=int, default=365, help="Days to look back for most traded pairs")
    parser.add_argument("--limit", "-l", type=int, default=100, help="Limit number of pairs for most traded")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    config = Config(args.config)
    
    # Override config with command line arguments if provided
    if args.pairs:
        config.override('base_symbols', args.pairs.split(','))
        config.set_all_pairs(False)
    if args.timeframes:
        config.override('timeframes', args.timeframes.split(','))
    
    downloader = CandleDataDownloader(config)
    
    if args.most_traded:
        downloader.download_candles_for_pairs(most_traded=True, days=args.days, limit=args.limit)
    else:
        downloader.download_candles_for_pairs()
