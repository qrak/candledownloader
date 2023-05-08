from classdir.candledownloader import CandleDownloader

if __name__ == "__main__":

    # Enable logging. Default: False
    enable_logging = False

    # Choose exchange, tested on binance
    exchange_name = 'binance'

    # Create lists of base symbols, quote symbols, and timeframes
    base_symbols = ['BTC', 'ETH', 'ADA', 'DOT', 'XRP', 'SOL', 'MATIC', 'LTC', 'AVAX', 'LINK', 'ATOM', 'ETC']
    quote_symbols = ['USDT']

    # timeframes available: '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '12h', '1d', '3d', '1w', '1M'
    timeframes = ['5m', '15m', '1h', '1d', '1w', '1M']

    # Iterate over the base symbols, quote symbols, and timeframes and download candles for each pair
    for base_symbol in base_symbols:
        for quote_symbol in quote_symbols:
            for timeframe in timeframes:
                pair_name = f"{base_symbol}/{quote_symbol}"
                candledownload = CandleDownloader(exchange_name=exchange_name, pair_name=pair_name, timeframe=timeframe,
                                                  log_to_file=enable_logging)
                candledownload.logger.info(f"Downloading candles for {pair_name}... timeframe: {timeframe}")
                candledownload.download_candles()
                candledownload.logger.info(f"Finished downloading candles for {pair_name} with timeframe {timeframe}\n")

