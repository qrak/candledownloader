import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Union
from configparser import ConfigParser

import ccxt
import numpy as np
import pandas as pd


def average_quote_volume(close_prices: Union[List[float], np.ndarray],
                         volumes: Union[List[float], np.ndarray],
                         window_size: int = 96) -> float:
    """
    Calculate the average quote volume based on the close prices and volumes over a specified window size.

    Parameters:
    close_prices (Union[List[float], np.ndarray]): List or array of close prices for each period.
    volumes (Union[List[float], np.ndarray]): List or array of trading volumes for each period.
    window_size (int): Size of the rolling window for calculating the average. Default is 96.

    Returns:
    float: The overall average quote volume.

    Raises:
    ValueError: If the lengths of close_prices and volumes are not equal.
    """
    if len(close_prices) != len(volumes):
        raise ValueError("The lengths of close_prices and volumes must be equal.")

    close_prices = np.array(close_prices)
    volumes = np.array(volumes)

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
    A class for downloading and managing candle data from cryptocurrency exchanges.

    This class provides functionality to fetch historical price data (candles) for various
    trading pairs across different timeframes. It can be configured to download data for
    specific pairs or all available pairs on an exchange.

    Attributes:
        exchange_name (str): The name of the exchange to fetch data from.
        all_pairs (bool): Flag to indicate whether to fetch data for all available trading pairs.
        base_symbols (List[str]): List of base symbols to fetch data for when not using all pairs.
        quote_symbols (List[str]): List of quote symbols to fetch data for when not using all pairs.
        start_time (str): The start time for fetching data in ISO 8601 format.
        end_time (Optional[str]): The end time for fetching data in ISO 8601 format.
        batch_size (int): The number of candles to fetch in each API request.
        output_directory (str): The directory where output CSV files will be stored.
        output_file (Optional[str]): The name of the output file (if a single file is used for all data).
        timeframes (List[str]): List of timeframes to fetch data for (e.g., ['1h', '1d', '1w']).
        log_to_file (bool): Flag to indicate whether to log output to a file.
        exchange (ccxt.Exchange): The CCXT exchange object used for API calls.
        stablecoins (List[str]): List of stablecoin symbols to exclude from base currencies.
        trading_pairs (List[str]): List of trading pairs to download data for.
    """

    def __init__(self, cfg: ConfigParser):
        """
        Initialize the CandleDataDownloader with the specified configuration.

        Parameters:
            cfg (ConfigParser): Configuration object containing the necessary parameters.
        """
        self.exchange_name: str = cfg.get('DEFAULT', 'exchange_name')
        self.all_pairs: bool = cfg.getboolean('DEFAULT', 'all_pairs')
        self.base_symbols: List[str] = cfg.get('DEFAULT', 'base_symbols').split(',')
        self.quote_symbols: List[str] = cfg.get('DEFAULT', 'quote_symbols').split(',')
        self.timeframes: List[str] = cfg.get('DEFAULT', 'timeframes').split(',')
        self.start_time: str = cfg.get('DEFAULT', 'start_time')
        self.end_time: Optional[str] = cfg.get('DEFAULT', 'end_time') or None
        self.batch_size: int = cfg.getint('DEFAULT', 'batch_size')
        self.output_directory: str = cfg.get('DEFAULT', 'output_directory')
        self.output_file: Optional[str] = cfg.get('DEFAULT', 'output_file') or None
        self.log_to_file: bool = cfg.getboolean('DEFAULT', 'enable_logging')

        self.trading_pairs: List[str] = []
        self.exchange: ccxt.Exchange = getattr(ccxt, self.exchange_name)()
        self.stablecoins: List[str] = [
            'USDT', 'BUSD', 'USDC', 'DAI', 'USD', 'EUR', 'JPY', 'GBP', 'AUD', 'CAD', 'CHF', 'CNY',
            'SEK', 'NZD', 'PLN', 'TUSD', 'PAX', 'USDS', 'FDUSD', 'USDP', 'MOB', 'USTC', 'EURT', 'RUB', 'BRL',
            'GUSD', 'USDJ', 'USDD', 'EURS', 'INR', 'HKD', 'EUROC', 'PYUSD', 'USDe', 'XBT', 'ZUSD',
            'ZEUR', 'ZCAD', 'ZJPY', 'ZGBP', 'ZAUD', 'USDK', 'USDX', 'USDE',
            'AEUR', 'ARS', 'BIDR', 'BKRW', 'BVND', 'COP', 'CZK', 'IDRT', 'MXN', 'NGN', 'RON', 'SBTC',
            'SUSDT', 'TRY', 'UAH', 'USD4', 'UST', 'VAI', 'ZAR'
        ]

    def _get_all_pairs(self, quote_currency: str = 'USDT') -> List[str]:
        """
        Retrieve all active spot trading pairs for a given quote currency.

        This method filters the available markets on the exchange to return only
        active spot trading pairs with the specified quote currency. It excludes
        pairs where the base currency is a stablecoin.

        Parameters:
        quote_currency (str): The quote currency to filter pairs by. Default is 'USDT'.

        Returns:
        List[str]: A list of trading pair symbols in the format "BASE/QUOTE".

        Raises:
        ccxt.NetworkError: If there's an issue connecting to the exchange API.
        """
        markets = self.exchange.load_markets()
        active_spot_pairs = [
            f"{market['base']}/{quote_currency}"
            for market in markets.values()
            if market['quote'] == quote_currency and market['active'] and market.get('spot', False) and
               market['base'] not in self.stablecoins
        ]
        return active_spot_pairs

    def fetch_and_rank_pairs_by_volume(self, days: int = 365, limit: int = 100, quote_currency: str = 'USDT') -> List[
        str]:
        """
        Fetch and rank trading pairs by their average quote volume over a specified period.

        Parameters:
        days (int): The number of days to look back for volume data. Default is 365.
        limit (int): The maximum number of pairs to return. Default is 100.
        quote_currency (str): The quote currency to filter pairs by. Default is 'USDT'.

        Returns:
        List[str]: A list of trading pair symbols, ranked by average quote volume,
                   limited to the specified number of pairs.

        Raises:
        ccxt.NetworkError: If there's an issue connecting to the exchange API.
        ValueError: If the exchange does not support fetching OHLCV data.
        """
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days)
        markets = self.exchange.load_markets()
        volume_ranked_pairs: Dict[str, float] = {}

        for symbol, market in markets.items():
            if (market['active'] and market['spot'] and market['quote'] == quote_currency and
                    market['base'] not in self.stablecoins):
                try:
                    ohlcv = self.exchange.fetch_ohlcv(symbol, '1d',
                                                      since=self.exchange.parse8601(start_time.isoformat()), limit=days)
                    if ohlcv and len(ohlcv) > 0:
                        close_prices = np.array([candle[4] for candle in ohlcv])
                        volumes = np.array([candle[5] for candle in ohlcv])
                        average_volume = average_quote_volume(close_prices, volumes)
                        volume_ranked_pairs[symbol] = average_volume
                    else:
                        print(f"No data returned for {symbol} on {self.exchange.name}")
                except Exception as e:
                    print(f"Failed to fetch or calculate volume for {symbol}: {e}")

        most_traded_pairs = sorted(volume_ranked_pairs, key=volume_ranked_pairs.get, reverse=True)[:limit]
        return most_traded_pairs

    def download_candles_for_pairs(self, most_traded: bool = False, days: Optional[int] = None,
                                   limit: Optional[int] = None) -> None:
        """
        Download candles for trading pairs based on specified criteria.

        This method determines which trading pairs to download data for, based on the
        configuration of the CandleDataDownloader instance and the provided parameters.
        It then initiates the download process for each pair and timeframe.

        Parameters:
        most_traded (bool): If True, fetch and use the most traded pairs. Default is False.
        days (int, optional): Number of days to consider when ranking pairs by volume.
                              Only used if most_traded is True.
        limit (int, optional): Maximum number of pairs to download data for.
                               Only used if most_traded is True.

        Returns:
        None

        Raises:
        ValueError: If most_traded is True but days or limit is not provided.
        ccxt.NetworkError: If there's an issue connecting to the exchange API.
        """
        if most_traded:
            if days is None or limit is None:
                raise ValueError("Both 'days' and 'limit' must be provided when most_traded is True.")
            self.trading_pairs = self.fetch_and_rank_pairs_by_volume(days=days, limit=limit)
        elif self.all_pairs:
            self.trading_pairs = self._get_all_pairs()
        else:
            self.trading_pairs = [f"{base}/{quote}" for base in self.base_symbols for quote in self.quote_symbols]

        for pair_name in self.trading_pairs:
            for timeframe in self.timeframes:
                candledownload = CandleDownloader(
                    exchange_name=self.exchange_name,
                    pair_name=pair_name,
                    timeframe=timeframe,
                    start_time=self.start_time,
                    end_time=self.end_time,
                    batch_size=self.batch_size,
                    output_directory=self.output_directory,
                    output_file=self.output_file,
                    log_to_file=self.log_to_file
                )
                candledownload.download_candles()


class CandleDownloader:
    logger = None
    TIMEFRAME_TO_SECONDS: Dict[str, int] = {
        "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
        "1h": 3600, "2h": 7200, "3h": 10800, "4h": 14400,
        "6h": 21600, "12h": 43200, "1d": 86400, "1w": 604800,
    }

    @staticmethod
    def initialize_logger(log_to_file: bool = False) -> None:
        """
        Initialize the logger for the CandleDownloader class.

        This method sets up a logger with both console and file handlers (if specified).
        It's designed to be called only once to avoid duplicate handlers.

        Parameters:
        log_to_file (bool): If True, log messages will also be written to a file.
                            Default is False.

        Returns:
        None
        """
        if CandleDownloader.logger is not None:
            return

        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        if log_to_file:
            file_handler = logging.FileHandler('candle_downloader.log')
            file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)

        CandleDownloader.logger = logger

    def __init__(self, exchange_name: str = 'binance', pair_name: str = 'BTC/USDT', timeframe: str = '5m',
                 start_time: str = '2015-01-01T00:00:00Z', end_time: Optional[str] = None, batch_size: int = 1000,
                 output_directory: str = './csv_ohlcv', output_file: Optional[str] = None, log_to_file: bool = False):
        """
        Initialize a CandleDownloader instance.

        This method sets up the CandleDownloader with the specified parameters for
        downloading historical price data (candles) from a cryptocurrency exchange.

        Parameters:
        exchange_name (str): Name of the exchange to use. Default is 'binance'.
        pair_name (str): Trading pair to download data for. Default is 'BTC/USDT'.
        timeframe (str): Timeframe of the candles. Default is '5m'.
        start_time (str): Start time for data download in ISO 8601 format. Default is '2015-01-01T00:00:00Z'.
        end_time (str, optional): End time for data download in ISO 8601 format. Default is None (current time).
        batch_size (int): Number of candles to fetch in each API request. Default is 1000.
        output_directory (str): Directory to save the downloaded data. Default is './csv_ohlcv'.
        output_file (str, optional): Specific filename for the output. If None, a filename will be generated.
        log_to_file (bool): Whether to log messages to a file. Default is False.

        Raises:
        ValueError: If the pair_name or timeframe is not supported by the exchange.
        ccxt.NetworkError: If there's an issue connecting to the exchange API.
        """

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

    def generate_output_filename(self) -> str:
        """
        Generate a filename for the output CSV file.

        This method creates a directory (if it doesn't exist) and generates a filename
        based on the trading pair, timeframe, date range, and exchange name.

        Returns:
        str: The full path of the generated output filename.

        Raises:
        OSError: If there's an issue creating the output directory.
        """
        os.makedirs(self.output_directory, exist_ok=True)
        symbol_base = self.pair_name.split('/')[0]
        symbol_quote = self.pair_name.split('/')[1]
        start_date = self.start_time.split('T')[0]
        end_date = self.end_time.split('T')[0] if self.end_time else 'now'
        filename = f'{symbol_base}_{symbol_quote}_{self.timeframe}_{start_date}_{end_date}_{self.exchange.id}.csv'
        return os.path.join(self.output_directory, filename)

    def write_to_output_file(self, df: pd.DataFrame) -> None:
        """
        Write the downloaded candle data to the output CSV file.

        This method appends the new data to the existing file if it exists,
        or creates a new file if it doesn't exist.

        Parameters:
        df (pd.DataFrame): The DataFrame containing the candle data to be written.

        Raises:
        IOError: If there's an issue writing to the output file.
        """
        df.to_csv(self.output_file, mode='a', index=False, header=not os.path.exists(self.output_file))

    def download_candles(self) -> None:
        """
        Download historical candle data for the specified trading pair and timeframe.

        This method fetches candle data in batches, processes it, and writes it to a CSV file.
        It handles rate limiting and other potential errors during the download process.

        Raises:
        ccxt.NetworkError: If there's an issue connecting to the exchange API.
        ccxt.ExchangeError: If there's an error returned by the exchange.
        IOError: If there's an issue reading from or writing to files.
        """
        try:
            df = pd.read_csv(self.output_file, usecols=[0], header=None, skiprows=1)
            last_timestamp = int(df.iloc[-1, 0])
            self.logger.info(f"Last timestamp in file: {last_timestamp}")
        except (FileNotFoundError, pd.errors.EmptyDataError):
            last_timestamp = self.exchange.parse8601(self.start_time)
            self.logger.info(f"Starting from timestamp: {last_timestamp}")

        current_timestamp = self.get_current_timeframe_timestamp()
        self.logger.info(f"Current timeframe timestamp: {current_timestamp}")

        if last_timestamp >= current_timestamp:
            self.logger.info(f"Data for {self.timeframe} is up to date. Skipping download.")
            return

        self.start_time = last_timestamp + (self.exchange.parse_timeframe(self.timeframe) * 1000)

        while self.start_time < current_timestamp:
            try:
                ohlcvs = self.exchange.fetch_ohlcv(self.pair_name, self.timeframe, since=self.start_time,
                                                   limit=self.batch_size)

                # Drop the last candle as it might be incomplete
                if ohlcvs:
                    ohlcvs = ohlcvs[:-1]

                if not ohlcvs:
                    self.logger.info(f"No new data available for {self.pair_name}, timeframe: {self.timeframe}")
                    break

                df = pd.DataFrame(ohlcvs, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

                self.total_candles += len(df)
                self.total_batches += 1
                self.logger.info(
                    f"Downloaded {len(df)} candles for {self.pair_name}, timeframe: {self.timeframe} in batch {self.total_batches}")

                self.write_to_output_file(df)
                self.start_time = ohlcvs[-1][0] + self.exchange.parse_timeframe(self.timeframe) * 1000

            except (ccxt.RateLimitExceeded, ccxt.DDoSProtection) as e:
                self.logger.warning(f"Rate limit exceeded: {e}. Retrying in 60 seconds...")
                time.sleep(60)
            except ccxt.BaseError as e:
                self.logger.error(f"Exception occurred: {e}. Retrying in 60 seconds...")
                time.sleep(60)

        if self.total_candles == 0:
            self.logger.info(f"No new data downloaded for {self.pair_name}, timeframe: {self.timeframe}")
        else:
            self.logger.info(
                f'Download complete. Total new candles: {self.total_candles}, Total batches: {self.total_batches}, Output file: {self.output_file}')

    def get_current_timeframe_timestamp(self) -> int:
        now = datetime.now(timezone.utc)
        if self.timeframe == '1w':
            # Align to the start of the week (Monday)
            start_of_week = now - timedelta(days=now.weekday())
            return int(start_of_week.replace(hour=0, minute=0, second=0, microsecond=0,
                                             tzinfo=timezone.utc).timestamp() * 1000)
        elif self.timeframe == '1d':
            # Align to the start of the day
            return int(now.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc).timestamp() * 1000)
        else:
            # For other timeframes, align to the start of the current timeframe period
            seconds = self.TIMEFRAME_TO_SECONDS[self.timeframe]
            return int((now.timestamp() // seconds) * seconds * 1000)
