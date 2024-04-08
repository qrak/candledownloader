from datetime import datetime, timedelta
import ccxt
import pandas as pd
import time
import os
import logging
import numpy as np


def average_quote_volume(close_prices, volumes, window_size=96):
    """
    Calculate the average quote volume based on the close prices and volumes over a specified window size.

    Parameters:
    close_prices (list): List of close prices.
    volumes (list): List of volumes.
    window_size (int): Size of the window for calculating the average.

    Returns:
    float: The overall average quote volume.
    """
    n = len(close_prices)
    quote_volumes = np.full(n, np.nan)
    for i in range(window_size - 1, n):
        average_close_price = np.mean(close_prices[i - window_size + 1:i + 1])
        average_volume = np.mean(volumes[i - window_size + 1:i + 1])
        quote_volumes[i] = average_close_price * average_volume
    overall_average_quote_volume = np.nanmean(quote_volumes)
    return overall_average_quote_volume


class CandleDataDownloader:
    """
    Initialize the data fetcher with the specified parameters.

    Parameters:
        exchange_name (str): The name of the exchange.
        all_pairs (bool): A flag to indicate whether to fetch data for all available trading pairs.
        base_symbols (list): A list of base symbols to fetch data for.
        quote_symbols (list): A list of quote symbols to fetch data for.
        start_time (str): The start time for fetching data.
        end_time (str): The end time for fetching data.
        batch_size (int): The size of each data batch to fetch.
        output_directory (str): The directory where the output CSV files will be stored.
        output_file (str): The name of the output file.
        timeframes (list): A list of timeframes to fetch data for.
        log_to_file (bool): A flag to indicate whether to log output to a file.
    """
    def __init__(self, exchange_name='binance', all_pairs=True, base_symbols=None, quote_symbols=None,
                 start_time='2015-01-01T00:00:00Z', end_time=None, batch_size=1000,
                 output_directory='./csv_ohlcv', output_file=None,
                 timeframes=None, log_to_file=False):
        self.exchange_name = exchange_name
        self.all_pairs = all_pairs
        self.base_symbols = base_symbols if base_symbols else []
        self.quote_symbols = quote_symbols if quote_symbols else ['USDT']
        self.start_time = start_time
        self.end_time = end_time
        self.batch_size = batch_size
        self.output_directory = output_directory
        self.output_file = output_file
        self.timeframes = timeframes if timeframes else ['1h', '1d', '1w', '1M']
        self.trading_pairs = []
        self.log_to_file = log_to_file
        self.exchange = getattr(ccxt, exchange_name)()

    def get_all_pairs(self, quote_currency='USDT'):
        markets = self.exchange.load_markets()
        # Filter for active spot markets with the specified quote currency
        active_spot_pairs = [
            f"{market['base']}/{quote_currency}"
            for market in markets.values()
            if market['quote'] == quote_currency and market['active'] and market.get('spot', False)
        ]
        return active_spot_pairs

    def fetch_and_rank_pairs_by_volume(self, days=365, limit=100, quote_currency='USDT'):
        print("Fetching and ranking pairs by volume...")
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days)
        markets = self.exchange.load_markets()
        volume_ranked_pairs = {}

        # Define a list of stablecoins to skip as base currencies
        stablecoins = ['USDT', 'USDC', 'TUSD', 'PAX', 'BUSD', 'DAI', 'FDUSD']

        for symbol, market in markets.items():
            # Check if the market is active, is a spot market, has the specified quote currency,
            # and the base currency is not a stablecoin
            if (market['active'] and market['spot'] and market['quote'] == quote_currency and
                    market['base'] not in stablecoins):
                try:
                    print(f"Fetching OHLCV for {symbol}...")
                    ohlcv = self.exchange.fetch_ohlcv(symbol, '1d',
                                                      since=self.exchange.parse8601(start_time.isoformat()), limit=days)
                    if ohlcv:
                        close_prices = np.array([candle[4] for candle in ohlcv])
                        volumes = np.array([candle[5] for candle in ohlcv])
                        average_volume = average_quote_volume(close_prices, volumes)
                        volume_ranked_pairs[symbol] = average_volume
                except Exception as e:
                    print(f"Failed to fetch or calculate volume for {symbol}: {e}")

        # Sort pairs by average volume and select the top 'limit'
        most_traded_pairs = sorted(volume_ranked_pairs, key=volume_ranked_pairs.get, reverse=True)[:limit]
        print(
            f"Top {limit} most traded pairs with quote currency {quote_currency} (excluding stablecoin bases): {most_traded_pairs}")
        return most_traded_pairs

    def download_candles_for_pairs(self, most_traded=False, days=365, limit=100):
        """
        Download candles for trading pairs based on specified criteria.

        Args:
            most_traded (bool): Flag to indicate if the most traded pairs should be used.
            days (int): Number of days to consider for fetching and ranking pairs by volume.
            limit (int): Maximum number of pairs to consider for fetching and ranking.

        Returns:
            None
        """
        if most_traded:
            self.trading_pairs = self.fetch_and_rank_pairs_by_volume(days=days, limit=limit)
        else:
            if self.all_pairs:
                self.trading_pairs = self.get_all_pairs()
            else:
                self.trading_pairs = [f"{base}/{quote}" for base in self.base_symbols for quote in self.quote_symbols]

        # Iterate over the trading pairs and timeframes and download candles for each pair
        for pair_name in self.trading_pairs:
            for timeframe in self.timeframes:
                candledownload = CandleDownloader(exchange_name=self.exchange_name, pair_name=pair_name,
                                                  timeframe=timeframe, start_time=self.start_time,
                                                  end_time=self.end_time, batch_size=self.batch_size,
                                                  output_directory=self.output_directory, output_file=self.output_file,
                                                  log_to_file=self.log_to_file)
                candledownload.logger.info(f"Downloading candles for {pair_name}... timeframe: {timeframe}")
                candledownload.download_candles()
                candledownload.logger.info(f"Finished downloading candles for {pair_name} with timeframe {timeframe}\n")


class CandleDownloader:
    logger = None

    @staticmethod
    def initialize_logger(log_to_file=False):
        if CandleDownloader.logger is not None:
            return

        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        # Console handler
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        # File handler
        if log_to_file:
            file_handler = logging.FileHandler('candle_downloader.log')
            file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)

        CandleDownloader.logger = logger
        """
        Initializes the object with the specified parameters.

        Parameters:
            exchange_name (str): The name of the exchange (default is 'binance').
            pair_name (str): The trading pair name (default is 'BTC/USDT').
            timeframe (str): The time frame for the OHLCV data (default is '5m').
            start_time (str): The start time for fetching the data (default is '2015-01-01T00:00:00Z').
            end_time (str): The end time for fetching the data (default is None).
            batch_size (int): The batch size for fetching the data (default is 1000).
            output_directory (str): The directory for storing the CSV files (default is './csv_ohlcv').
            output_file (str): The name of the output file (default is None).
            log_to_file (bool): Flag indicating whether to log to a file (default is False).

        Returns:
            None
        """
    def __init__(self, exchange_name='binance', pair_name='BTC/USDT', timeframe='5m',
                 start_time='2015-01-01T00:00:00Z', end_time=None, batch_size=1000,
                 output_directory='./csv_ohlcv', output_file=None, log_to_file=False):
        self.exchange = getattr(ccxt, exchange_name)(
            {
                "enableRateLimit": True,
            })
        self.pair_name = pair_name
        self.timeframe = timeframe
        self.start_time = start_time
        self.end_time = end_time
        self.batch_size = batch_size
        self.output_directory = output_directory
        self.output_file = output_file
        self.total_candles = 0
        self.total_batches = 0
        self.buffer = []
        self.initialize_logger(log_to_file)

        # Validate the parameters
        if pair_name not in self.exchange.load_markets():
            raise ValueError(f"Invalid pair name: {pair_name}")
        if timeframe not in self.exchange.timeframes:
            raise ValueError(f"Invalid timeframe: {timeframe}")

        # Set the filename and path for the output file
        if output_file is None:
            self.output_file = self.generate_output_filename()

    def generate_output_filename(self):
        os.makedirs(self.output_directory, exist_ok=True)
        symbol_base = self.pair_name.split('/')[0]
        symbol_quote = self.pair_name.split('/')[1]
        start_date = self.start_time.split('T')[0]
        end_date = self.end_time.split('T')[0] if self.end_time else 'now'
        filename = f'{symbol_base}_{symbol_quote}_{self.timeframe}_{start_date}_{end_date}_{self.exchange.id}.csv'
        return os.path.join(self.output_directory, filename)

    def write_to_output_file(self, df):
        if os.path.isfile(self.output_file):
            with open(self.output_file, mode='a', newline='') as f:
                if f.tell() == 0:
                    df.to_csv(f, index=False, header=True)
                else:
                    df.to_csv(f, index=False, header=False)
        else:
            df.to_csv(self.output_file, index=False, header=True)

    def download_candles(self):
        # Check if the file already exists
        try:
            df = pd.read_csv(self.output_file, usecols=[0], header=None, skiprows=1)
            self.start_time = int(df.iloc[-1, 0]) + (
                    self.exchange.parse_timeframe(self.timeframe) * 1000)
            self.logger.info(f"Resuming from timestamp {self.start_time}...")
        except (FileNotFoundError, pd.errors.EmptyDataError):
            self.start_time = self.exchange.parse8601(self.start_time)

        # Fetch the candles and write them to the output file
        while True:
            try:
                # Fetch the candles for the current time range
                candles = self.exchange.fetch_ohlcv(self.pair_name, self.timeframe, since=self.start_time,
                                                    limit=self.batch_size)

                # Convert the fetched candles to a pandas dataframe
                df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

                if len(candles) == 0:
                    break

                # Append the dataframe to the in-memory buffer
                self.buffer.append(df)

                # Update the start time for the next request
                self.start_time = candles[-1][0] + self.exchange.parse_timeframe(self.timeframe) * 1000

                # Update progress message
                self.total_candles += len(df)
                self.total_batches += 1
                self.logger.info(f"Downloaded {self.total_candles} candles for {self.pair_name}, timeframe: {self.timeframe} in {self.total_batches} batches...")

                # Write the accumulated data to the output file
                self.write_to_output_file(df)

                # Clear the buffer
                self.buffer = []

            except (ccxt.RateLimitExceeded, ccxt.DDoSProtection) as e:
                self.logger.warning(f"Rate limit exceeded: {e}. Retrying in 60 seconds...")
                time.sleep(60)
            except ccxt.BaseError as e:
                self.logger.error(f"Exception occurred: {e}. Retrying in 60 seconds...")
                time.sleep(60)

                # Write the remaining data in the buffer to the output file
                if len(self.buffer) > 0:
                    df = pd.concat(self.buffer)
                    self.write_to_output_file(df)
                    self.buffer = []

        # Print a message when the script has finished
        self.logger.info(
            f'Download complete. Total candles: {self.total_candles}, Total batches: {self.total_batches}, Output file: {self.output_file}')