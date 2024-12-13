from datetime import datetime

from classdir.candledownloader import CandleDataDownloader, Config, TimeframeValidator

if __name__ == "__main__":
    config = Config('config.cfg')
    downloader = CandleDataDownloader(config)
    downloader.download_candles_for_pairs()

    # Validate downloaded files
    for pair_name in downloader.trading_pairs:
        for timeframe in config.timeframes:
            symbol_base, symbol_quote = pair_name.split('/')
            filename = (
                f'{config.output_directory}/'
                f'{symbol_base}_{symbol_quote}_{timeframe}_'
                f'{datetime.now().strftime("%Y-%m-%d")}_now_{downloader.exchange.exchange.id}.csv'
            )

            validator = TimeframeValidator(filename, timeframe)
            validator.validate()
