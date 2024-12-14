from src.candledownloader import CandleDataDownloader, Config

if __name__ == "__main__":
    config = Config('config.cfg')
    downloader = CandleDataDownloader(config)
    downloader.download_candles_for_pairs()
