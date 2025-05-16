import os
import time
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any

import ccxt
import numpy as np

from src.config import Config
from src.data_manager import DataManager
from src.timeframe_manager import TimeframeManager
from utils.average_quote_vol import AverageVolumeCalculator


# Fix the progress bar to prevent it from moving down
class ProgressBar:
    def __init__(self, total: int, prefix: str = '', length: int = 30):
        self.total = max(1, total)  # Prevent division by zero
        self.prefix = prefix
        self.length = length
        self.current = 0
        self.start_time = time.time()
        self.last_update_time = 0
        self.update_interval = 0.2  # Slightly faster updates
        
    def update(self, current: int) -> None:
        self.current = min(current, self.total)  # Prevent overflow
        
        # Throttle visual updates to avoid flickering
        current_time = time.time()
        if current_time - self.last_update_time < self.update_interval and current < self.total:
            return
            
        self.last_update_time = current_time
        progress = self.current / self.total
        blocks = int(self.length * progress)
        bar = '█' * blocks + '░' * (self.length - blocks)
        elapsed = current_time - self.start_time
        
        if progress > 0:
            eta = (elapsed / progress) * (1 - progress)
            eta_str = f"ETA: {timedelta(seconds=int(eta))}"
        else:
            eta_str = "ETA: calculating..."
        
        # Simplified output - avoid using \033[K which can cause flickering
        output = f"\r{self.prefix} |{bar}| {int(progress*100)}% {current}/{self.total} {eta_str}"
        
        # Pad with spaces to ensure overwriting of previous longer lines
        terminal_width = self._get_terminal_width()
        if terminal_width > 0:
            padding = ' ' * max(0, terminal_width - len(output) - 5)
            output += padding
            
        sys.stdout.write(output)
        sys.stdout.flush()
        
    def finish(self) -> None:
        elapsed = time.time() - self.start_time
        output = f"\r{self.prefix} |{'█' * self.length}| 100% {self.total}/{self.total} Complete in {timedelta(seconds=int(elapsed))}"
        
        # Add padding and newline
        terminal_width = self._get_terminal_width()
        if terminal_width > 0:
            padding = ' ' * max(0, terminal_width - len(output) - 5)
            output += padding
            
        sys.stdout.write(output + "\n")
        sys.stdout.flush()
    
    def _get_terminal_width(self) -> int:
        """Get terminal width safely"""
        try:
            import os
            terminal_size = os.get_terminal_size()
            return terminal_size.columns
        except (AttributeError, OSError, ImportError):
            return 80  # Default fallback width


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
                        print(f"No data returned for {symbol} on {self.exchange.exchange.id}")
                except Exception as e:
                    print(f"Failed to fetch or calculate volume for {symbol}: {e}")

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
                    output_file=self.config.output_file
                )
                downloader.download_candles()


class CandleDownloader:
    def __init__(self, exchange_interface: ExchangeInterface, pair_name: str, timeframe: str,
                 start_time: str, end_time: Optional[str] = None, batch_size: int = 1000,
                 output_directory: str = './csv_ohlcv', output_file: Optional[str] = None,
                 buffer_size: int = 10000):

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
        # Set quiet mode to true during progress bar display
        self.data_manager = DataManager(self.output_file, quiet=True)
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
            print(f"Starting download for {self.pair_name} ({self.timeframe}) at {start_time}")
            self._download_new_data()
            
            if self.total_candles > 0:
                avg_candles_per_batch = self.total_candles / self.total_batches
                print(f"Download completed for {self.pair_name} ({self.timeframe}). "
                      f"Total candles: {self.total_candles}, "
                      f"Total batches: {self.total_batches}, "
                      f"Average candles per batch: {int(avg_candles_per_batch)}")
            return True
        except Exception as e:
            print(f"Error downloading candles: {str(e)}")
            return False

    def _download_new_data(self):
        last_timestamp = self.data_manager.get_last_timestamp() or self.start_time
        target_timestamp = self.end_time if self.end_time else TimeframeManager.get_current_timestamp(self.timeframe)

        if last_timestamp >= target_timestamp:
            print(f"Data for {self.pair_name} {self.timeframe} is up to date. Skipping download.")
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
              
        print(f"Starting download from {datetime.fromtimestamp(start_time/1000).strftime('%Y-%m-%d %H:%M:%S')} "
              f"to {datetime.fromtimestamp(target_timestamp/1000).strftime('%Y-%m-%d %H:%M:%S')}. "
              f"Estimated batches to download: {estimated_total_batches}")
        
        progress = ProgressBar(
            estimated_total_batches, 
            prefix=f"Downloading {self.pair_name} ({self.timeframe})"
        )
        
        retry_count = 0
        max_retries = 5
        
        try:
            # Save original stdout to restore it later if needed
            original_stdout = sys.stdout
            
            while start_time < target_timestamp:
                try:
                    ohlcvs = self.exchange.fetch_ohlcv(
                        self.pair_name, self.timeframe,
                        since=start_time, limit=self.batch_size
                    )

                    if not ohlcvs:
                        print(f"Failed to fetch {self.pair_name}, timeframe: {self.timeframe}")
                        break

                    if self.end_time:
                        ohlcvs = [candle for candle in ohlcvs if candle[0] <= self.end_time]

                    if not ohlcvs:
                        break

                    ohlcvs = ohlcvs[:-1]
                    self.data_manager.data_buffer.extend(ohlcvs)

                    if len(self.data_manager.data_buffer) >= self.buffer_size:
                        # Temporarily redirect stdout to suppress any potential output
                        # Just clear the line, but don't print anything
                        sys.stdout.write("\r\033[K")
                        sys.stdout.flush()
                        
                        # Buffer writing (should be silent with quiet=True)
                        self.data_manager.write_buffer()
                        
                        # Update progress bar after buffer is written
                        progress.update(self.total_batches)

                    self.total_candles += len(ohlcvs)
                    self.total_batches += 1
                    
                    progress.update(self.total_batches)
                    
                    current_time = datetime.fromtimestamp(ohlcvs[-1][0]/1000).strftime('%Y-%m-%d %H:%M:%S')
                    # We're not logging but can optionally display details
                    # print(f"Batch {self.total_batches}/{estimated_total_batches} - "
                    #       f"Downloaded {len(ohlcvs)} candles for {self.pair_name} "
                    #       f"(timeframe: {self.timeframe}). Latest candle time: {current_time}")

                    start_time = ohlcvs[-1][0] + timeframe_ms
                    retry_count = 0

                except (ccxt.RateLimitExceeded, ccxt.DDoSProtection) as e:
                    retry_count += 1
                    wait_time = min(60 * retry_count, 300)
                    
                    # Clear line completely before printing message
                    sys.stdout.write("\r\033[K")
                    sys.stdout.flush()
                    print(f"Rate limit exceeded. Retrying in {wait_time} seconds... (Attempt {retry_count}/{max_retries})")
                    
                    if retry_count >= max_retries:
                        print(f"Maximum retries reached. Skipping to next timeframe.")
                        break
                        
                    time.sleep(wait_time)
                    progress.update(self.total_batches)
                    continue
                    
                except ccxt.BaseError as e:
                    # Use the same approach for other error handlers
                    sys.stdout.write("\r\033[K")
                    sys.stdout.flush()
                    print(f"Exchange error occurred: {str(e)[:50]}... Retrying in {wait_time} seconds...")
                    
                    retry_count += 1
                    wait_time = min(60 * retry_count, 300)
                    
                    if retry_count >= max_retries:
                        print(f"Maximum retries reached. Skipping to next timeframe.")
                        break
                    
                    time.sleep(wait_time)
                    progress.update(self.total_batches)
                    continue
                    
                except Exception as e:
                    sys.stdout.write("\r\033[K")
                    sys.stdout.flush()
                    print(f"Unexpected error: {str(e)[:50]}... Skipping to next timeframe.")
                    break

            # Ensure progress bar is properly finished
            progress.finish()
            
            # Make sure we're at the start of a new line after the progress bar
            sys.stdout.write("\n")
            sys.stdout.flush()
            
            # After progress bar is complete, we can print buffer write info
            if self.data_manager.data_buffer:
                self.data_manager.write_buffer()
            
            if self.total_candles == 0:
                print(f"No new data downloaded for {self.pair_name}, timeframe: {self.timeframe}")
            else:
                completion_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                total_msg = (
                    f'Download complete. Total new candles: {self.total_candles}, '
                    f'Batches: {self.total_batches}/{estimated_total_batches}'
                )
                print(f"\n{total_msg}")
                print(f'Download complete at {completion_time}. Output file: {self.output_file}')
        finally:
            # Ensure we have a clean slate for the next operation
            sys.stdout.write("\n")
            sys.stdout.flush()