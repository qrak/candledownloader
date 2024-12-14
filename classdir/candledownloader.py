from configparser import ConfigParser
from typing import List, Dict, Optional, Union, Any
from datetime import datetime, timedelta, timezone
import logging
import os
import time
import ccxt
import numpy as np
import pandas as pd

class AverageVolumeCalculator:
    @staticmethod
    def calculate(close_prices: Union[List[float], np.ndarray],
                 volumes: Union[List[float], np.ndarray],
                 window_size: int = 96) -> float:
        if len(close_prices) != len(volumes):
            raise ValueError("The lengths of close_prices and volumes must be equal.")

        close_prices = np.array(close_prices)
        volumes = np.array(volumes)
        n = len(close_prices)
        quote_volumes = np.full(n, np.nan)

        for i in range(window_size - 1, n):
            window_slice = slice(i - window_size + 1, i + 1)
            average_close_price = np.mean(close_prices[window_slice])
            average_volume = np.mean(volumes[window_slice])
            quote_volumes[i] = average_close_price * average_volume

        return float(np.nanmean(quote_volumes))


class ExchangeInterface:
    def __init__(self, exchange_name: str):
        self.exchange: ccxt.Exchange = getattr(ccxt, exchange_name)({'enableRateLimit': True})
        self.stablecoins: List[str] = [
            'USDT', 'BUSD', 'USDC', 'DAI', 'USD', 'EUR', 'JPY', 'GBP', 'AUD', 'CAD', 'CHF', 'CNY',
            'SEK', 'NZD', 'PLN', 'TUSD', 'PAX', 'USDS', 'FDUSD', 'USDP', 'MOB', 'USTC', 'EURT', 'RUB', 'BRL',
            'GUSD', 'USDJ', 'USDD', 'EURS', 'INR', 'HKD', 'EUROC', 'PYUSD', 'USDe', 'XBT', 'ZUSD',
            'ZEUR', 'ZCAD', 'ZJPY', 'ZGBP', 'ZAUD', 'USDK', 'USDX', 'USDE',
            'AEUR', 'ARS', 'BIDR', 'BKRW', 'BVND', 'COP', 'CZK', 'IDRT', 'MXN', 'NGN', 'RON', 'SBTC',
            'SUSDT', 'TRY', 'UAH', 'USD4', 'UST', 'VAI', 'ZAR'
        ]

    def get_active_pairs(self, quote_currency: str = 'USDT') -> List[str]:
        markets = self.exchange.load_markets()
        return [
            f"{market['base']}/{quote_currency}"
            for market in markets.values()
            if market['quote'] == quote_currency and market['active'] and
            market.get('spot', False) and market['base'] not in self.stablecoins
        ]

    def fetch_ohlcv(self, symbol: str, timeframe: str, since: int, limit: int) -> Any:
        return self.exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)


class TimeframeHandler:
    TIMEFRAME_TO_SECONDS: Dict[str, int] = {
        "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
        "1h": 3600, "2h": 7200, "3h": 10800, "4h": 14400,
        "6h": 21600, "12h": 43200, "1d": 86400, "1w": 604800,
    }

    @staticmethod
    def get_timeframe_in_seconds(timeframe: str) -> int:
        return TimeframeHandler.TIMEFRAME_TO_SECONDS[timeframe]

    @staticmethod
    def get_current_timestamp(timeframe: str) -> int:
        now = datetime.now(timezone.utc)
        if timeframe == '1w':
            start_of_week = now - timedelta(days=now.weekday())
            return int(start_of_week.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
        elif timeframe == '1d':
            return int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
        else:
            timeframe_seconds = TimeframeHandler.TIMEFRAME_TO_SECONDS[timeframe]
            current_timestamp = int(now.timestamp() * 1000)
            return current_timestamp - (current_timestamp % (timeframe_seconds * 1000))

    def __init__(self, csv_file: str, timeframe: str):
        self.csv_file = csv_file
        self.timeframe = timeframe
        self.expected_interval = self.TIMEFRAME_TO_SECONDS[timeframe] * 1000
        self.df = pd.read_csv(csv_file)
        self.logger = LoggerManager.setup_logger(
            f"{__name__}.TimeframeHandler",
            True,
            f'timeframe_handler_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        )

    def validate(self) -> Dict[str, Any]:
        timestamps = self.df['timestamp'].values
        gaps: List[Dict[str, int]] = []
        invalid_intervals: List[Dict[str, int]] = []

        for i in range(1, len(timestamps)):
            interval = timestamps[i] - timestamps[i - 1]

            if interval > self.expected_interval:
                gaps.append({
                    'start': int(timestamps[i - 1]),
                    'end': int(timestamps[i]),
                    'gap_size': int(interval)
                })
            elif interval != self.expected_interval:
                invalid_intervals.append({
                    'position': i,
                    'timestamp': int(timestamps[i]),
                    'interval': int(interval)
                })

        result = {
            'file': self.csv_file,
            'timeframe': self.timeframe,
            'total_records': len(timestamps),
            'gaps_count': len(gaps),
            'invalid_intervals_count': len(invalid_intervals),
            'gaps': gaps,
            'invalid_intervals': invalid_intervals,
            'is_valid': len(gaps) == 0 and len(invalid_intervals) == 0
        }

        self._log_results(result)
        return result

    def _log_results(self, result: Dict[str, Any]) -> None:
        self.logger.info(f"Validation results for {self.csv_file}:")
        self.logger.info(f"Timeframe: {self.timeframe}")
        self.logger.info(f"Total records: {result['total_records']}")
        self.logger.info(f"Gaps found: {result['gaps_count']}")
        self.logger.info(f"Invalid intervals: {result['invalid_intervals_count']}")

        if result['gaps']:
            self.logger.warning("Gaps detected:")
            for gap in result['gaps']:
                start_time = datetime.fromtimestamp(gap['start'] / 1000)
                end_time = datetime.fromtimestamp(gap['end'] / 1000)
                gap_td = timedelta(milliseconds=gap['gap_size'])
                self.logger.warning(
                    f"Gap from {start_time} ({gap['start']}) to "
                    f"{end_time} ({gap['end']}) "
                    f"(size: {gap_td})"
                )

        if result['invalid_intervals']:
            self.logger.warning("Invalid intervals detected:")
            for interval in result['invalid_intervals']:
                time = datetime.fromtimestamp(interval['timestamp'] / 1000)
                interval_td = timedelta(milliseconds=interval['interval'])
                self.logger.warning(
                    f"Position {interval['position']}: {time} "
                    f"({interval['timestamp']}) "
                    f"(interval: {interval_td})"
                )

        self.logger.info(f"Validation {'passed' if result['is_valid'] else 'failed'}")


class LoggerManager:
    @staticmethod
    def setup_logger(name: str, log_to_file: bool = False, filename: Optional[str] = None) -> logging.Logger:
        logger = logging.getLogger(name)
        if logger.hasHandlers():
            return logger
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter('%(message)s', datefmt='%Y-%m-%d %H:%M:%S')

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        if log_to_file and filename:
            file_handler = logging.FileHandler(filename)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        return logger


class DataManager:
    def __init__(self, output_file: str):
        self.output_file = output_file
        self.data_buffer: List[Any] = []

    def get_last_timestamp(self) -> int:
        try:
            df = pd.read_csv(self.output_file, usecols=[0], header=None, skiprows=1)
            return int(df.iloc[-1, 0])
        except (FileNotFoundError, pd.errors.EmptyDataError):
            return 0

    def write_buffer(self) -> None:
        if self.data_buffer:
            df = pd.DataFrame(self.data_buffer,
                              columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df.to_csv(self.output_file, mode='a', index=False,
                      header=not os.path.exists(self.output_file))
            self.data_buffer.clear()


class Config:
    def __init__(self, config_file: str = 'config.cfg'):
        self.cfg = ConfigParser()
        self.cfg.read(config_file)

    @property
    def exchange_name(self) -> str:
        return self.cfg.get('DEFAULT', 'exchange_name')

    @property
    def all_pairs(self) -> bool:
        return self.cfg.getboolean('DEFAULT', 'all_pairs')

    @property
    def base_symbols(self) -> List[str]:
        return self.cfg.get('DEFAULT', 'base_symbols').split(',')

    @property
    def quote_symbols(self) -> List[str]:
        return self.cfg.get('DEFAULT', 'quote_symbols').split(',')

    @property
    def timeframes(self) -> List[str]:
        return self.cfg.get('DEFAULT', 'timeframes').split(',')

    @property
    def start_time(self) -> str:
        return self.cfg.get('DEFAULT', 'start_time')

    @property
    def end_time(self) -> Optional[str]:
        return self.cfg.get('DEFAULT', 'end_time') or None

    @property
    def batch_size(self) -> int:
        return self.cfg.getint('DEFAULT', 'batch_size')

    @property
    def output_directory(self) -> str:
        return self.cfg.get('DEFAULT', 'output_directory')

    @property
    def output_file(self) -> Optional[str]:
        return self.cfg.get('DEFAULT', 'output_file') or None

    @property
    def log_to_file(self) -> bool:
        return self.cfg.getboolean('DEFAULT', 'enable_logging')

class CandleDataDownloader:
    def __init__(self, config: Config):
        self.config = config
        self.exchange = ExchangeInterface(config.exchange_name)
        self.trading_pairs: List[str] = []
        self.logger = LoggerManager.setup_logger(
            f"{__name__}.CandleDataDownloader",
            self.config.log_to_file,
            'candle_data_downloader.log'
        )



    def fetch_and_rank_pairs_by_volume(self, days: int = 365, limit: int = 100) -> List[str]:
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days)
        markets = self.exchange.exchange.load_markets()
        volume_ranked_pairs: Dict[str, float] = {}

        for symbol, market in markets.items():
            if (market['active'] and market.get('spot', False) and
                    market['quote'] in self.config.quote_symbols and
                    market['base'] not in self.exchange.stablecoins):
                try:
                    ohlcv = self.exchange.fetch_ohlcv(
                        symbol, '1d',
                        since=self.exchange.exchange.parse8601(start_time.isoformat()),
                        limit=days
                    )

                    if ohlcv and len(ohlcv) > 0:
                        close_prices = np.array([candle[4] for candle in ohlcv])
                        volumes = np.array([candle[5] for candle in ohlcv])
                        average_volume = AverageVolumeCalculator.calculate(close_prices, volumes)
                        volume_ranked_pairs[symbol] = average_volume
                    else:
                        self.logger.info(
                            f"No data returned for {symbol} on {self.exchange.exchange.id}"
                        )
                except Exception as e:
                    self.logger.error(f"Failed to fetch or calculate volume for {symbol}: {e}")

        most_traded_pairs = sorted(
            volume_ranked_pairs,
            key=volume_ranked_pairs.get,
            reverse=True
        )[:limit]

        return most_traded_pairs

    def download_candles_for_pairs(self, most_traded: bool = False, days: Optional[int] = None,
                                   limit: Optional[int] = None) -> None:
        if most_traded:
            if days is None or limit is None:
                raise ValueError("Both 'days' and 'limit' must be provided when most_traded is True.")
            self.trading_pairs = self.fetch_and_rank_pairs_by_volume(days=days, limit=limit)
        elif self.config.all_pairs:
            self.trading_pairs = self.exchange.get_active_pairs()
        else:
            self.trading_pairs = [f"{base}/{quote}"
                                  for base in self.config.base_symbols
                                  for quote in self.config.quote_symbols]

        for pair_name in self.trading_pairs:
            for timeframe in self.config.timeframes:
                downloader = CandleDownloader(
                    exchange_interface=self.exchange,
                    pair_name=pair_name,
                    timeframe=timeframe,
                    start_time=self.config.start_time,
                    end_time=self.config.end_time,
                    batch_size=self.config.batch_size,
                    output_directory=self.config.output_directory,
                    output_file=self.config.output_file,
                    log_to_file=self.config.log_to_file
                )
                downloader.download_candles()


class CandleDownloader:
    def __init__(self, exchange_interface: ExchangeInterface, pair_name: str, timeframe: str,
                 start_time: str, end_time: Optional[str] = None, batch_size: int = 1000,
                 output_directory: str = './csv_ohlcv', output_file: Optional[str] = None,
                 log_to_file: bool = False, buffer_size: int = 10000):

        self.exchange = exchange_interface
        self.pair_name = pair_name
        self.timeframe = timeframe
        self.start_time = self.exchange.exchange.parse8601(start_time)
        self.end_time = self.exchange.exchange.parse8601(end_time) if end_time else None
        self.batch_size = batch_size
        self.buffer_size = buffer_size
        self.total_candles = 0
        self.total_batches = 0

        self.output_file = output_file or self._generate_output_filename(output_directory)
        self.data_manager = DataManager(self.output_file)
        self.logger = LoggerManager.setup_logger(
            f"{__name__}.{self.pair_name}.{self.timeframe}",
            log_to_file,
            f'candle_downloader_{self.pair_name}_{self.timeframe}.log' if log_to_file else None
        )
        self._validate_inputs()

    def _generate_output_filename(self, output_directory: str) -> str:
        os.makedirs(output_directory, exist_ok=True)
        symbol_base, symbol_quote = self.pair_name.split('/')
        start_date = datetime.fromtimestamp(self.start_time / 1000).strftime('%Y-%m-%d')
        end_date = ('now' if self.end_time is None else
                    datetime.fromtimestamp(self.end_time / 1000).strftime('%Y-%m-%d'))
        filename = (f'{symbol_base}_{symbol_quote}_{self.timeframe}_'
                    f'{start_date}_{end_date}_{self.exchange.exchange.id}.csv')
        return os.path.join(output_directory, filename)

    def _setup_logger(self, log_to_file: bool) -> logging.Logger:
        logger = logging.getLogger(f"{__name__}.{self.pair_name}.{self.timeframe}")
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        if log_to_file:
            file_handler = logging.FileHandler(
                f'candle_downloader_{self.pair_name}_{self.timeframe}.log'
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        return logger

    def _validate_inputs(self) -> None:
        if self.pair_name not in self.exchange.exchange.load_markets():
            raise ValueError(f"Invalid pair name: {self.pair_name}")
        if self.timeframe not in self.exchange.exchange.timeframes:
            raise ValueError(f"Invalid timeframe: {self.timeframe}")

    def download_candles(self) -> None:
        # First, validate existing data
        validator = TimeframeHandler(self.output_file, self.timeframe)
        validation_result = validator.validate()

        if validation_result['gaps']:
            self.logger.info(f"Found {validation_result['gaps_count']} gaps. Attempting to fill them...")
            self._fill_gaps(validation_result['gaps'])

        # Continue with regular download from the last timestamp
        self._download_new_data()

    def _fill_gaps(self, gaps: List[Dict[str, int]]) -> None:
        for gap in gaps:
            start_time = gap['start']
            end_time = gap['end']

            self.logger.info(
                f"Attempting to fill gap from "
                f"{datetime.fromtimestamp(start_time / 1000)} to "
                f"{datetime.fromtimestamp(end_time / 1000)}"
            )

            current_start = start_time
            while current_start < end_time:
                try:
                    ohlcvs = self.exchange.fetch_ohlcv(
                        self.pair_name, self.timeframe,
                        since=current_start, limit=self.batch_size
                    )

                    if not ohlcvs:
                        self.logger.error(
                            f"Failed to fetch gap data for {self.pair_name}, "
                            f"timeframe: {self.timeframe}"
                        )
                        break

                    # Filter candles within the gap
                    gap_candles = [
                        candle for candle in ohlcvs
                        if start_time < candle[0] < end_time
                    ]

                    if gap_candles:
                        # Read existing data
                        df = pd.read_csv(self.output_file)

                        # Convert gap candles to DataFrame
                        gap_df = pd.DataFrame(
                            gap_candles,
                            columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
                        )

                        # Combine and sort data
                        combined_df = pd.concat([df, gap_df])
                        combined_df = combined_df.drop_duplicates(subset=['timestamp'])
                        combined_df = combined_df.sort_values('timestamp')

                        # Save back to file
                        combined_df.to_csv(self.output_file, index=False)

                        self.logger.info(
                            f"Filled {len(gap_candles)} candles in gap"
                        )

                    current_start = ohlcvs[-1][0] + (
                            self.exchange.exchange.parse_timeframe(self.timeframe) * 1000
                    )

                except (ccxt.RateLimitExceeded, ccxt.DDoSProtection) as e:
                    self.logger.warning(f"Rate limit exceeded: {e}. Retrying in 60 seconds...")
                    time.sleep(60)
                except ccxt.BaseError as e:
                    self.logger.error(f"Exception occurred: {e}. Retrying in 60 seconds...")
                    time.sleep(60)
                    break

    def _download_new_data(self) -> None:
        last_timestamp = self.data_manager.get_last_timestamp() or self.start_time
        current_timestamp = TimeframeHandler.get_current_timestamp(self.timeframe)

        if last_timestamp >= current_timestamp:
            self.logger.info(
                f"Data for {self.pair_name} {self.timeframe} is up to date. Skipping download."
            )
            return

        start_time = last_timestamp + (
                self.exchange.exchange.parse_timeframe(self.timeframe) * 1000
        )

        while start_time < current_timestamp:
            try:
                ohlcvs = self.exchange.fetch_ohlcv(
                    self.pair_name, self.timeframe,
                    since=start_time, limit=self.batch_size
                )

                if not ohlcvs:
                    self.logger.error(
                        f"Failed to fetch {self.pair_name}, timeframe: {self.timeframe}"
                    )
                    break

                ohlcvs = ohlcvs[:-1]
                self.data_manager.data_buffer.extend(ohlcvs)

                if len(self.data_manager.data_buffer) >= self.buffer_size:
                    self.data_manager.write_buffer()

                self.total_candles += len(ohlcvs)
                self.total_batches += 1
                self.logger.info(
                    f"Downloaded {len(ohlcvs)} candles for {self.pair_name}, "
                    f"timeframe: {self.timeframe} in batch {self.total_batches}"
                )

                start_time = ohlcvs[-1][0] + (
                        self.exchange.exchange.parse_timeframe(self.timeframe) * 1000
                )

            except (ccxt.RateLimitExceeded, ccxt.DDoSProtection) as e:
                self.logger.warning(f"Rate limit exceeded: {e}. Retrying in 60 seconds...")
                time.sleep(60)
                self._download_new_data()
            except ccxt.BaseError as e:
                self.logger.error(f"Exception occurred: {e}. Retrying in 60 seconds...")
                time.sleep(60)

        self.data_manager.write_buffer()

        if self.total_candles == 0:
            self.logger.info(
                f"No new data downloaded for {self.pair_name}, timeframe: {self.timeframe}"
            )
        else:
            self.logger.info(
                f'Download complete. Total new candles: {self.total_candles}, '
                f'Total batches: {self.total_batches}, Output file: {self.output_file}'
            )
