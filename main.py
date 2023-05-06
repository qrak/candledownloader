from classdir.candledownloader import CandleDownloader


if __name__ == "__main__":

    # Choose exchange
    exchange_name = 'binance'
    timeframe = '5m'

    # Create lists of base symbols and quote symbols
    base_symbols = ['BTC', 'ETH', 'ADA', 'DOT', 'XRP', 'SOL', 'MATIC', 'LTC', 'AVAX', 'LINK', 'ATOM', 'ETC']
    quote_symbols = ['USDT']

    # Iterate over the base symbols and quote symbols and download candles for each pair
    for base_symbol in base_symbols:
        for quote_symbol in quote_symbols:
            pair_name = f"{base_symbol}/{quote_symbol}"
            print(f"Downloading candles for {pair_name}... timeframe: {timeframe}")
            candledownload = CandleDownloader(exchange_name=exchange_name, pair_name=pair_name, timeframe=timeframe)
            candledownload.download_candles()
            print(f"Finished downloading candles for {pair_name}\n")

