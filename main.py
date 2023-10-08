from classdir.candledownloader import CandleDataDownloader
from configparser import ConfigParser

if __name__ == "__main__":
    """
    This script serves as an entry point to initiate the process of downloading candlestick data
    for specified or all trading pairs from a given exchange, using the `CandleDataDownloader` class
    defined in the `classdir.candledownloader` module. The script offers the flexibility to specify
    the base and quote currencies, timeframes, an option to enable or disable logging, and whether to
    download data for all available trading pairs or just the specified ones.

    The `CandleDataDownloader` class interfaces with the `ccxt` library to interact with cryptocurrency
    exchanges and download candlestick data, which is further handled by the `CandleDownloader` class to
    organize and store the data into CSV files.

    Usage:
        1. Adjust the `all_pairs` variable to True if you want to download candle data for all trading pairs
           available on the exchange or to False if you want to download data for specific pairs.
        2. If `all_pairs` is set to False, adjust the `base_symbols` list to include the desired base currencies.
        3. Optionally, adjust the `quote_symbols` and `timeframes` list to customize the quote currencies
           and timeframes respectively.
        4. Set `enable_logging` to True if logging is desired.
        5. Run the script.

    Parameters:
        all_pairs (bool): Flag to indicate whether to download data for all trading pairs or just the specified ones.
                          Default is True.
        enable_logging (bool): Flag to enable or disable logging. Default is False.
        base_symbols (list): List of base currencies for which to download candle data. Default list includes
                             popular cryptocurrencies like BTC, ETH, ADA etc.
        quote_symbols (list): List of quote currencies. Default is ['USDT'].
        timeframes (list): List of timeframes for which to download candle data. Default is ['1h', '1d', '1w', '1M'].
        start_time (str): The starting time for which to download candle data in ISO 8601 format. Default is '2015-01-01T00:00:00Z'.
        end_time (str): The ending time for which to download candle data in ISO 8601 format. Default is None.
        batch_size (int): The number of records to be fetched in each request. Default is 1000.
        output_directory (str): The directory where CSV files will be stored. Default is './csv_ohlcv'.
        output_file (str): The name of the output file. If specified, data will be stored in this file. Default is None.

    Classes:
        CandleDataDownloader: Handles the configuration and initiation of candle data download process.
        CandleDownloader: Manages the downloading, buffering, and storing of candle data.

    Dependencies:
        - ccxt: Library for cryptocurrency trading.
        - pandas: Data manipulation and analysis library.
        - logging: Logging library for tracking events during runtime.


    Note:
        Ensure the `ccxt` library is installed and the `classdir.candledownloader` module is accessible
        from the script's location.
    """

    # Read configurations from config.cfg
    config = ConfigParser()
    config.read('config.cfg')

    all_pairs = config.getboolean('DEFAULT', 'all_pairs')
    base_symbols = config.get('DEFAULT', 'base_symbols').split(',')
    quote_symbols = config.get('DEFAULT', 'quote_symbols').split(',')
    timeframes = config.get('DEFAULT', 'timeframes').split(',')
    start_time = config.get('DEFAULT', 'start_time')
    end_time = config.get('DEFAULT', 'end_time') or None  # It will be None if not specified
    batch_size = config.getint('DEFAULT', 'batch_size')
    output_directory = config.get('DEFAULT', 'output_directory')
    output_file = config.get('DEFAULT', 'output_file') or None  # It will be None if not specified
    enable_logging = config.getboolean('DEFAULT', 'enable_logging')

    # Instantiate CandleDataDownloader class
    downloader = CandleDataDownloader(all_pairs=all_pairs,
                                      base_symbols=base_symbols,
                                      quote_symbols=quote_symbols,
                                      timeframes=timeframes,
                                      start_time=start_time,
                                      end_time=end_time,
                                      batch_size=batch_size,
                                      output_directory=output_directory,
                                      output_file=None,
                                      log_to_file=enable_logging)

    # Call the method to download candles for the specified or all trading pairs
    downloader.download_candles_for_pairs()
