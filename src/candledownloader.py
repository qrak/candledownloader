import os
import time
import sys
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any

import ccxt
import numpy as np

from src.config import Config
from src.data_manager import DataManager
from src.logger_manager import LoggerManager
from src.timeframe_manager import TimeframeManager
from utils.average_quote_vol import AverageVolumeCalculator


class ProgressBar:
    def __init__(self, total: int, prefix: str = '', length: int = 30):
        self.total = total
        self.prefix = prefix
        self.length = length
        self.current = 0
        self.start_time = time.time()
        self.last_update_time = 0
        self.update_interval = 0.5
        
    def update(self, current: int) -> None:
        self.current = current
        
        # Throttle visual updates to avoid excessive screen refresh
        current_time = time.time()
        if current_time - self.last_update_time < self.update_interval and current < self.total:
            return
            
        self.last_update_time = current_time
        progress = min(1.0, current / self.total)
        blocks = int(self.length * progress)
        bar = '█' * blocks + '░' * (self.length - blocks)
        elapsed = current_time - self.start_time
        
        if progress > 0:
            eta = (elapsed / progress) * (1 - progress)
            eta_str = f"ETA: {timedelta(seconds=int(eta))}"
        else:
            eta_str = "ETA: calculating..."
            
        sys.stdout.write(f"\r{self.prefix} |{bar}| {int(progress*100)}% {current}/{self.total} {eta_str}")
        sys.stdout.flush()
        
    def finish(self) -> None:
        elapsed = time.time() - self.start_time
        sys.stdout.write(f"\r{self.prefix} |{'█' * self.length}| 100% {self.total}/{self.total} Complete in {timedelta(seconds=int(elapsed))}")
        sys.stdout.write("\n") 
        sys.stdout.flush()


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

    def _validate_inputs(self) -> None:
        if self.pair_name not in self.exchange.exchange.load_markets():
            raise ValueError(f"Invalid pair name: {self.pair_name}")
        if self.timeframe not in self.exchange.exchange.timeframes:
            raise ValueError(f"Invalid timeframe: {self.timeframe}")

    def download_candles(self):
        try:
            start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger.info(
                f"Starting download for {self.pair_name} ({self.timeframe}) at {start_time}"
            )
            self._download_new_data()
            
            if self.total_candles > 0:
                avg_candles_per_batch = self.total_candles / self.total_batches
                self.logger.info(
                    f"Download completed for {self.pair_name} ({self.timeframe}). "
                    f"Total candles: {self.total_candles}, "
                    f"Total batches: {self.total_batches}, "
                    f"Average candles per batch: {int(avg_candles_per_batch)}"
                )
            return True
        except Exception as e:
            self.logger.error(f"Error downloading candles: {str(e)}")
            return False

    def _download_new_data(self):
        last_timestamp = self.data_manager.get_last_timestamp() or self.start_time
        target_timestamp = self.end_time if self.end_time else TimeframeManager.get_current_timestamp(self.timeframe)

        if last_timestamp >= target_timestamp:
            self.logger.info(
                f"Data for {self.pair_name} {self.timeframe} is up to date. Skipping download."
            )
            return

        start_time = last_timestamp + (
                self.exchange.exchange.parse_timeframe(self.timeframe) * 1000
        )

        timeframe_ms = self.exchange.exchange.parse_timeframe(self.timeframe) * 1000
        total_time_range = target_timestamp - start_time
        estimated_total_batches = max(1, int(total_time_range / (timeframe_ms * self.batch_size)))
        
        print(f"\nDownloading {self.pair_name} ({self.timeframe}) from "
              f"{datetime.fromtimestamp(start_time/1000).strftime('%Y-%m-%d %H:%M:%S')} "
              f"to {datetime.fromtimestamp(target_timestamp/1000).strftime('%Y-%m-%d %H:%M:%S')}")
              
        self.logger.info(
            f"Starting download from {datetime.fromtimestamp(start_time/1000).strftime('%Y-%m-%d %H:%M:%S')} "
            f"to {datetime.fromtimestamp(target_timestamp/1000).strftime('%Y-%m-%d %H:%M:%S')}. "
            f"Estimated batches to download: {estimated_total_batches}"
        )
        
        progress = ProgressBar(
            estimated_total_batches, 
            prefix=f"Downloading {self.pair_name} ({self.timeframe})"
        )

        logger_level = self.logger.level
        console_handlers = []

        for handler in self.logger.handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
                console_handlers.append(handler)
                handler.setLevel(logging.CRITICAL)
        
        retry_count = 0
        max_retries = 5
        
        try:
            while start_time < target_timestamp:
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

                    if self.end_time:
                        ohlcvs = [candle for candle in ohlcvs if candle[0] <= self.end_time]

                    if not ohlcvs:
                        break

                    ohlcvs = ohlcvs[:-1]
                    self.data_manager.data_buffer.extend(ohlcvs)

                    if len(self.data_manager.data_buffer) >= self.buffer_size:
                        self.data_manager.write_buffer()

                    self.total_candles += len(ohlcvs)
                    self.total_batches += 1
                    
                    progress.update(self.total_batches)
                    
                    current_time = datetime.fromtimestamp(ohlcvs[-1][0]/1000).strftime('%Y-%m-%d %H:%M:%S')
                    self.logger.info(
                        f"Batch {self.total_batches}/{estimated_total_batches} - "
                        f"Downloaded {len(ohlcvs)} candles for {self.pair_name} "
                        f"(timeframe: {self.timeframe}). Latest candle time: {current_time}"
                    )

                    start_time = ohlcvs[-1][0] + timeframe_ms
                    retry_count = 0

                except (ccxt.RateLimitExceeded, ccxt.DDoSProtection) as e:
                    retry_count += 1
                    wait_time = min(60 * retry_count, 300)
                    
                    # Clear current line and show error
                    sys.stdout.write("\r" + " " * 100 + "\r")
                    print(f"Rate limit exceeded. Retrying in {wait_time} seconds... (Attempt {retry_count}/{max_retries})")
                    
                    self.logger.warning(f"Rate limit exceeded: {e}. Retrying in {wait_time} seconds... (Attempt {retry_count}/{max_retries})")
                    
                    if retry_count >= max_retries:
                        self.logger.error(f"Maximum retries reached. Skipping to next timeframe.")
                        break
                        
                    time.sleep(wait_time)
                    progress.update(self.total_batches)
                    continue
                    
                except ccxt.BaseError as e:
                    retry_count += 1
                    wait_time = min(60 * retry_count, 300)
                    
                    # Clear current line and show error
                    sys.stdout.write("\r" + " " * 100 + "\r")
                    print(f"Exchange error occurred: {str(e)[:50]}... Retrying in {wait_time} seconds...")
                    
                    self.logger.error(f"Exception occurred: {e}. Retrying in {wait_time} seconds... (Attempt {retry_count}/{max_retries})")
                    
                    if retry_count >= max_retries:
                        self.logger.error(f"Maximum retries reached. Skipping to next timeframe.")
                        break
                    
                    time.sleep(wait_time)
                    progress.update(self.total_batches)
                    continue
                    
                except Exception as e:
                    # Clear current line and show error
                    sys.stdout.write("\r" + " " * 100 + "\r")
                    print(f"Unexpected error: {str(e)[:50]}... Skipping to next timeframe.")
                    
                    self.logger.error(f"Unexpected error: {str(e)}. Skipping to next timeframe.")
                    break

            progress.finish()
            
            if self.data_manager.data_buffer:
                self.data_manager.write_buffer()

            if self.total_candles == 0:
                print(f"No new data downloaded for {self.pair_name}, timeframe: {self.timeframe}")
                self.logger.info(
                    f"No new data downloaded for {self.pair_name}, timeframe: {self.timeframe}"
                )
            else:
                completion_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                total_msg = (
                    f'Download complete. Total new candles: {self.total_candles}, '
                    f'Batches: {self.total_batches}/{estimated_total_batches}'
                )
                print(f"\n{total_msg}")
                
                self.logger.info(
                    f'Download complete at {completion_time}. '
                    f'Total new candles: {self.total_candles}, '
                    f'Total batches: {self.total_batches}/{estimated_total_batches}, '
                    f'Output file: {self.output_file}'
                )
        finally:
            # Restore console handlers' log levels
            for handler in console_handlers:
                handler.setLevel(logger_level)