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
    TIMEFRAME_TO_SECONDS: Dict[str, int] = {
        "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
        "1h": 3600, "2h": 7200, "3h": 10800, "4h": 14400,
        "6h": 21600, "12h": 43200, "1d": 86400, "1w": 604800,
    }

    def __init__(self, exchange_name: str, pair_name: str, timeframe: str,
                 start_time: str, end_time: Optional[str] = None, batch_size: int = 1000,
                 output_directory: str = './csv_ohlcv', output_file: Optional[str] = None,
                 log_to_file: bool = False, buffer_size: int = 10000):
        self.exchange = getattr(ccxt, exchange_name)({"enableRateLimit": True})
        self.pair_name = pair_name
        self.timeframe = timeframe
        self.start_time = self.exchange.parse8601(start_time)
        self.end_time = self.exchange.parse8601(end_time) if end_time else None
        self.batch_size = batch_size
        self.output_directory = output_directory
        self.output_file = output_file or self.generate_output_filename()
        self.buffer_size = buffer_size
        self.data_buffer = []
        self.total_candles = 0
        self.total_batches = 0

        self.logger = self.setup_logger(log_to_file)
        self.validate_inputs()

    def setup_logger(self, log_to_file: bool) -> logging.Logger:
        logger = logging.getLogger(f"{__name__}.{self.pair_name}.{self.timeframe}")
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S')

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        if log_to_file:
            file_handler = logging.FileHandler(f'candle_downloader_{self.pair_name}_{self.timeframe}.log')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        return logger

    def validate_inputs(self):
        if self.pair_name not in self.exchange.load_markets():
            raise ValueError(f"Invalid pair name: {self.pair_name}")
        if self.timeframe not in self.exchange.timeframes:
            raise ValueError(f"Invalid timeframe: {self.timeframe}")

    def generate_output_filename(self) -> str:
        os.makedirs(self.output_directory, exist_ok=True)
        symbol_base, symbol_quote = self.pair_name.split('/')
        start_date = datetime.fromtimestamp(self.start_time / 1000).strftime('%Y-%m-%d')
        end_date = 'now' if self.end_time is None else datetime.fromtimestamp(self.end_time / 1000).strftime('%Y-%m-%d')
        filename = f'{symbol_base}_{symbol_quote}_{self.timeframe}_{start_date}_{end_date}_{self.exchange.id}.csv'
        return os.path.join(self.output_directory, filename)

    def get_last_timestamp(self) -> int:
        try:
            df = pd.read_csv(self.output_file, usecols=[0], header=None, skiprows=1)
            return int(df.iloc[-1, 0])
        except (FileNotFoundError, pd.errors.EmptyDataError):
            return self.start_time

    def get_current_timeframe_timestamp(self) -> int:
        now = datetime.now(timezone.utc)
        if self.timeframe == '1w':
            start_of_week = now - timedelta(days=now.weekday())
            return int(start_of_week.replace(hour=0, minute=0, second=0, microsecond=0,
                                             tzinfo=timezone.utc).timestamp() * 1000)
        elif self.timeframe == '1d':
            return int(now.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc).timestamp() * 1000)
        else:
            seconds = self.TIMEFRAME_TO_SECONDS[self.timeframe]
            return int((now.timestamp() // seconds) * seconds * 1000)

    def write_buffer_to_file(self):
        if self.data_buffer:
            df = pd.DataFrame(self.data_buffer, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df.to_csv(self.output_file, mode='a', index=False, header=not os.path.exists(self.output_file))
            self.data_buffer.clear()

    def download_candles(self) -> None:
        last_timestamp = self.get_last_timestamp()
        current_timestamp = self.get_current_timeframe_timestamp()

        if last_timestamp >= current_timestamp:
            self.logger.info(f"Data for {self.pair_name} {self.timeframe} is up to date. Skipping download.")
            return

        start_time = last_timestamp + (self.exchange.parse_timeframe(self.timeframe) * 1000)

        while start_time < current_timestamp:
            try:
                ohlcvs = self.exchange.fetch_ohlcv(self.pair_name, self.timeframe, since=start_time,
                                                   limit=self.batch_size)

                if not ohlcvs:
                    self.logger.error(f"Failed to fetch {self.pair_name}, timeframe: {self.timeframe}")
                    break

                ohlcvs = ohlcvs[:-1]  # Remove the last (potentially incomplete) candle
                self.data_buffer.extend(ohlcvs)

                if len(self.data_buffer) >= self.buffer_size:
                    self.write_buffer_to_file()

                self.total_candles += len(ohlcvs)
                self.total_batches += 1
                self.logger.info(
                    f"Downloaded {len(ohlcvs)} candles for {self.pair_name}, timeframe: {self.timeframe} in batch {self.total_batches}")

                start_time = ohlcvs[-1][0] + self.exchange.parse_timeframe(self.timeframe) * 1000

            except (ccxt.RateLimitExceeded, ccxt.DDoSProtection) as e:
                self.logger.warning(f"Rate limit exceeded: {e}. Retrying in 60 seconds...")
                time.sleep(60)
                self.download_candles()
            except ccxt.BaseError as e:
                self.logger.error(f"Exception occurred: {e}. Retrying in 60 seconds...")
                time.sleep(60)

        self.write_buffer_to_file()  # Write any remaining data in the buffer

        if self.total_candles == 0:
            self.logger.info(f"No new data downloaded for {self.pair_name}, timeframe: {self.timeframe}")
        else:
            self.logger.info(
                f'Download complete. Total new candles: {self.total_candles}, Total batches: {self.total_batches}, Output file: {self.output_file}')
