import argparse
import logging
import os
import time
from collections import defaultdict
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple

import ccxt
import pandas as pd

from src.timeframe_manager import TimeframeManager
from src.logger_manager import LoggerManager
from src.config import Config

class MultiExchangeGapFiller:
    def __init__(self) -> None:
        self.exchanges = {
            'binance': ccxt.binance({'enableRateLimit': True}),
            'binanceusdm': ccxt.binanceusdm({'enableRateLimit': True}),
            'binanceus': ccxt.binanceus({'enableRateLimit': True}),
            'bitget': ccxt.bitget({'enableRateLimit': True}),
            'bitmart': ccxt.bitmart({'enableRateLimit': True}),
            'bitmex': ccxt.bitmex({'enableRateLimit': True}),
            'bybit': ccxt.bybit({'enableRateLimit': True}),
            'coinbase': ccxt.coinbase({'enableRateLimit': True}),
            'coinex': ccxt.coinex({'enableRateLimit': True}),
            'cryptocom': ccxt.cryptocom({'enableRateLimit': True}),
            'gate': ccxt.gate({'enableRateLimit': True}),
            'htx': ccxt.htx({'enableRateLimit': True}),
            'kucoin': ccxt.kucoin({'enableRateLimit': True}),
            'kucoinfutures': ccxt.kucoinfutures({'enableRateLimit': True}),
            'mexc': ccxt.mexc({'enableRateLimit': True}),
            'okx': ccxt.okx({'enableRateLimit': True}),
            'woo': ccxt.woo({'enableRateLimit': True}),
            'ascendex': ccxt.ascendex({'enableRateLimit': True}),
            'bitfinex': ccxt.bitfinex({'enableRateLimit': True}),
            'bitstamp': ccxt.bitstamp({'enableRateLimit': True}),
            'bitrue': ccxt.bitrue({'enableRateLimit': True}),
            'bitvavo': ccxt.bitvavo({'enableRateLimit': True}),
            'blockchaincom': ccxt.blockchaincom({'enableRateLimit': True}),
            'coinbaseinternational': ccxt.coinbaseinternational({'enableRateLimit': True}),
            'deribit': ccxt.deribit({'enableRateLimit': True}),
            'gemini': ccxt.gemini({'enableRateLimit': True}),
            'kraken': ccxt.kraken({'enableRateLimit': True}),
            'krakenfutures': ccxt.krakenfutures({'enableRateLimit': True}),
            'lbank': ccxt.lbank({'enableRateLimit': True}),
            'phemex': ccxt.phemex({'enableRateLimit': True}),
            'poloniex': ccxt.poloniex({'enableRateLimit': True}),
            'upbit': ccxt.upbit({'enableRateLimit': True}),
            'whitebit': ccxt.whitebit({'enableRateLimit': True})
        }


    def fetch_candles(self, exchange_name: str, pair: str, timeframe: str,
                      since: int, limit: int = 1000) -> List[List[Any]]:
        try:
            exchange = self.exchanges[exchange_name]
            if timeframe not in exchange.timeframes:
                print(f"Exchange {exchange_name} does not support timeframe {timeframe}")
                return []
            ohlcv = exchange.fetch_ohlcv(
                pair,
                timeframe,
                since=since,
                limit=limit
            )
            return ohlcv
        except Exception as e:
            print(f"Error fetching from {exchange_name}: {str(e)}")
            return []

class ValidationRunner:
    def __init__(self, directory: str, timeframe: Optional[str] = None,
                 pair: Optional[str] = None) -> None:
        self.directory = directory
        self.timeframe = timeframe
        self.pair = pair
        self.csv_files = self._get_csv_files()
        self.validation_results: Dict[str, Dict] = {}
        self.gap_filler = MultiExchangeGapFiller()
        self.config = Config()
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f'validation_runner_{timestamp}.log'

        return LoggerManager.setup_logger(
            f"{__name__}.ValidationRunner",
            self.config.log_to_file,
            log_file
        )

    def _get_csv_files(self) -> List[str]:
        all_files = os.listdir(self.directory)
        csv_files = [
            f for f in all_files
            if f.endswith('.csv') and
               (self.timeframe is None or f'_{self.timeframe}_' in f) and
               (self.pair is None or f.startswith(self.pair))
        ]
        return csv_files

    def _extract_pair_name(self, filename: str) -> str:
        # More robust pair name extraction
        try:
            # Split filename and remove extension
            name_parts = os.path.splitext(filename)[0].split('_')
            
            # Find the timeframe part index
            timeframe_idx = -1
            for i, part in enumerate(name_parts):
                if part in TimeframeManager.TIMEFRAME_TO_SECONDS:
                    timeframe_idx = i
                    break
            
            if timeframe_idx > 0:
                # Pair is everything before the timeframe
                base_quote = '_'.join(name_parts[:timeframe_idx])
                return base_quote
            else:
                # Fallback to original method
                return f"{name_parts[0]}_{name_parts[1]}"
        except Exception as e:
            self.logger.warning(f"Error extracting pair name from {filename}: {str(e)}")
            # Fallback to original method
            parts = filename.split('_')
            return f"{parts[0]}_{parts[1]}" if len(parts) > 1 else parts[0]

    def run_validation(self) -> None:
        for file in self.csv_files:
            file_path = os.path.join(self.directory, file)
            
            # First check if the file is corrupted
            integrity_check = self._check_file_integrity(file_path)
            if not integrity_check['is_valid']:
                print(f"\n⚠️ File integrity issues found in {file}:")
                for issue in integrity_check['issues']:
                    print(f"  - {issue}")
                continue
                
            timeframe = self._extract_timeframe(file)

            if timeframe:
                print(f"\nValidating {file}...")
                validator = TimeframeManager(file_path, timeframe)
                result = validator.validate()

                pair_name = self._extract_pair_name(file)
                self.validation_results[file] = {
                    'pair': pair_name,
                    'timeframe': timeframe,
                    'gaps_count': result['gaps_count'],
                    'gaps': result['gaps'],
                    'invalid_intervals': result['invalid_intervals_count'],
                    'integrity': integrity_check
                }

                if not result['is_valid']:
                    print(f"\nValidation issues found in {file}:")
                    if result['gaps']:
                        print(f"Gaps found: {result['gaps_count']}")
                    if result['invalid_intervals']:
                        print(f"Invalid intervals found: {result['invalid_intervals_count']}")
            else:
                print(f"Could not extract timeframe from filename: {file}")

        if not self.pair:
            self._print_summary()
            
    def _check_file_integrity(self, file_path: str) -> Dict[str, Any]:
        """
        Check if file is corrupted or has incomplete data due to interruptions (CTRL+C)
        """
        issues = []
        is_valid = True
        
        try:
            # Check 1: Can the file be opened and read?
            if not os.path.exists(file_path):
                issues.append("File does not exist")
                return {'is_valid': False, 'issues': issues}
                
            if os.path.getsize(file_path) == 0:
                issues.append("File is empty")
                return {'is_valid': False, 'issues': issues}
            
            # Check 2: Parse as CSV without errors
            try:
                df = pd.read_csv(file_path)
            except pd.errors.ParserError:
                issues.append("File is not a valid CSV format - possible corruption")
                return {'is_valid': False, 'issues': issues}
            except pd.errors.EmptyDataError:
                issues.append("File contains no data")
                return {'is_valid': False, 'issues': issues}
            
            # Check 3: Verify expected columns
            expected_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            missing_columns = [col for col in expected_columns if col not in df.columns]
            if missing_columns:
                issues.append(f"Missing required columns: {', '.join(missing_columns)}")
                is_valid = False
            
            # Check 4: Check for NaN values which might indicate truncated records
            nan_counts = df.isna().sum()
            nan_columns = [col for col in df.columns if nan_counts[col] > 0]
            if nan_columns:
                issues.append(f"Found NaN values in columns: {', '.join(nan_columns)} - possible incomplete write")
                is_valid = False
            
            # Check 5: Verify data consistency
            if all(col in df.columns for col in ['open', 'high', 'low', 'close']):
                # Check high >= low (should always be true)
                invalid_hl = (df['high'] < df['low']).sum()
                if invalid_hl > 0:
                    issues.append(f"Found {invalid_hl} records where 'high' is less than 'low' - data corruption")
                    is_valid = False
                
                # Check close is within high-low range
                invalid_close = ((df['close'] > df['high']) | (df['close'] < df['low'])).sum()
                if invalid_close > 0:
                    issues.append(f"Found {invalid_close} records where 'close' is outside high-low range - data corruption")
                    is_valid = False
            
            # Check 6: Verify timestamps are sorted (indicates potential missing end due to CTRL+C)
            if 'timestamp' in df.columns and len(df) > 1:
                if not all(df['timestamp'].diff().iloc[1:] > 0):
                    issues.append("Timestamps are not in ascending order - possible file corruption")
                    is_valid = False
                    
            # Check 7: Check for suspiciously truncated file (last record might be partially written)
            # If file doesn't end with newline or has an unexpected EOF marker
            with open(file_path, 'rb') as f:
                f.seek(0, os.SEEK_END)
                last_position = f.tell()
                f.seek(max(0, last_position - 100), os.SEEK_SET)  # Get last 100 bytes
                last_bytes = f.read()
                if not last_bytes.endswith(b'\n'):
                    issues.append("File doesn't end with a newline - possible incomplete write")
                    is_valid = False
                    
            return {'is_valid': is_valid, 'issues': issues}
            
        except Exception as e:
            issues.append(f"Error checking file integrity: {str(e)}")
            return {'is_valid': False, 'issues': issues}

    def _print_summary(self) -> None:
        if not self.validation_results:
            print("\nNo files were validated.")
            return

        pairs_with_issues = defaultdict(lambda: {'gaps': 0, 'files': []})

        for file, result in self.validation_results.items():
            if result['gaps'] > 0 or result['invalid_intervals'] > 0:
                pair = result['pair']
                pairs_with_issues[pair]['gaps'] += result['gaps']
                pairs_with_issues[pair]['files'].append(file)

        if pairs_with_issues:
            print("\nSummary of pairs with issues:")
            for pair, data in sorted(pairs_with_issues.items()):
                print(f"\n{pair}:")
                print(f"  Total gaps: {data['gaps']}")
                print("  Affected files:")
                for file in data['files']:
                    print(f"    - {file}")
        else:
            print("\nNo issues found in any pairs.")

    def fill_gaps(self) -> None:
        if not self.validation_results:
            self.logger.warning("No validation results available. Run validation first.")
            return

        for file, result in self.validation_results.items():
            if result['gaps_count'] > 0:
                self.logger.info(f"\nAttempting to fill gaps in {file}")
                pair = result['pair'].replace('_', '/')
                timeframe = result['timeframe']

                gaps_list = result['gaps']
                if not gaps_list:
                    continue

                self._fill_file_gaps(file, pair, timeframe, gaps_list)

    def _fill_file_gaps(self, file: str, pair: str, timeframe: str,
                        gaps_list: List[Dict[str, int]]) -> None:
        exchanges = list(self.gap_filler.exchanges.keys())
        file_path = os.path.join(self.directory, file)
        
        # Try both standard pair format and alternative formats
        pair_variations = self._get_pair_variations(pair)
        
        for gap in gaps_list:
            gap_filled = False
            interval_ms = TimeframeManager.get_timeframe_in_seconds(timeframe) * 1000
            num_candles = (gap['end'] - gap['start']) // interval_ms
            gap_start_year = datetime.fromtimestamp(gap['start'] / 1000).year

            coverage_threshold = 50 if gap_start_year < 2020 else 80

            self.logger.info(f"\nAttempting to fill gap from {datetime.fromtimestamp(gap['start'] / 1000)} "
                           f"to {datetime.fromtimestamp(gap['end'] / 1000)} "
                           f"(need {num_candles} candles)")

            for exchange_name in exchanges:
                try:
                    exchange = self.gap_filler.exchanges[exchange_name]

                    if timeframe not in exchange.timeframes:
                        continue

                    # Try each pair variation until one works
                    exchange_candles = []
                    used_pair = None
                    
                    for pair_variant in pair_variations:
                        try:
                            self.logger.info(f"Trying to fill gap using {exchange_name} with pair {pair_variant}")
                            
                            # Check if pair exists on exchange
                            markets = exchange.load_markets()
                            if pair_variant not in markets:
                                continue
                                
                            # Try to fetch a single candle to verify pair works
                            test_candle = self.gap_filler.fetch_candles(
                                exchange_name,
                                pair_variant,
                                timeframe,
                                gap['start'],
                                1
                            )
                            
                            if test_candle:
                                used_pair = pair_variant
                                break
                        except Exception as e:
                            self.logger.debug(f"Pair {pair_variant} not available on {exchange_name}: {str(e)}")
                            continue
                    
                    if not used_pair:
                        self.logger.warning(f"No valid pair format found for {pair} on {exchange_name}")
                        continue
                        
                    current_start = gap['start']
                    remaining_attempts = 3  # Limit retries for a specific exchange

                    while current_start < gap['end'] and remaining_attempts > 0:
                        try:
                            remaining_ms = gap['end'] - current_start
                            remaining_candles = remaining_ms // interval_ms
                            chunk_size = min(remaining_candles, 500)

                            ohlcv = self.gap_filler.fetch_candles(
                                exchange_name,
                                used_pair,
                                timeframe,
                                current_start,
                                int(chunk_size)
                            )

                            if not ohlcv:
                                self.logger.warning(f"No data returned from {exchange_name}")
                                remaining_attempts -= 1
                                time.sleep(2)
                                continue

                            valid_candles = [
                                candle for candle in ohlcv
                                if gap['start'] <= candle[0] <= gap['end']
                            ]

                            if valid_candles:
                                exchange_candles.extend(valid_candles)
                                current_start = valid_candles[-1][0] + interval_ms
                            else:
                                current_start += interval_ms * chunk_size

                            # Respect rate limits
                            time.sleep(exchange.rateLimit / 1000 * 1.5)

                        except ccxt.RateLimitExceeded:
                            self.logger.warning(f"Rate limit exceeded for {exchange_name}, waiting 60 seconds")
                            time.sleep(60)
                            remaining_attempts -= 1
                            continue
                        except Exception as e:
                            self.logger.error(f"Error fetching chunk from {exchange_name}: {str(e)}")
                            remaining_attempts -= 1
                            time.sleep(5)
                            continue

                    if exchange_candles:
                        self._process_gap_candles(file_path, exchange_name, exchange_candles, gap, interval_ms, coverage_threshold)
                        gap_filled = True
                        break

                except Exception as e:
                    self.logger.error(f"Error with exchange {exchange_name}: {str(e)}")
                    continue

            if not gap_filled:
                self.logger.warning("Failed to fill gap with any exchange")
                
    def _process_gap_candles(self, file_path: str, exchange_name: str, exchange_candles: List, 
                            gap: Dict[str, int], interval_ms: int, coverage_threshold: float) -> bool:
        """Process downloaded candles and update the file if coverage is sufficient"""
        try:
            df = pd.DataFrame(
                exchange_candles,
                columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
            )
            df = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp')

            expected_timestamps = set(range(
                gap['start'], 
                gap['end'] + interval_ms, 
                interval_ms
            ))
            actual_timestamps = set(df['timestamp'].values)
            missing_timestamps = expected_timestamps - actual_timestamps
            coverage = (len(expected_timestamps) - len(missing_timestamps)) / len(expected_timestamps) * 100

            self.logger.info(f"Gap coverage from {exchange_name}: {coverage:.2f}% ({len(missing_timestamps)} missing candles)")

            if coverage >= coverage_threshold:
                self._update_csv_with_gap_data(file_path, df.values.tolist())
                self.logger.info(f"Successfully filled gap with {coverage:.2f}% coverage using {exchange_name}")
                return True
            return False
        except Exception as e:
            self.logger.error(f"Error processing gap candles: {str(e)}")
            return False
                
    def _get_pair_variations(self, pair: str) -> List[str]:
        """Generate possible variations of the pair format for different exchanges"""
        # Original format like "BTC_USDT"
        if '_' not in pair:
            return [pair]
            
        base, quote = pair.split('_', 1)
        variations = [
            f"{base}/{quote}",  # BTC/USDT - standard CCXT format
            pair.replace('_', '/'),  # Also BTC/USDT
            pair,  # Original format BTC_USDT
            f"{base}{quote}",  # BTCUSDT - some exchanges use this format
            f"{base}-{quote}"  # BTC-USDT - some exchanges use this format
        ]
        return variations

    @staticmethod
    def _extract_timeframe(filename: str) -> Optional[str]:
        try:
            parts = filename.split('_')
            for part in parts:
                part_lower = part.lower()
                if part_lower.endswith(('m', 'h', 'd', 'w')):
                    if part_lower in TimeframeManager.TIMEFRAME_TO_SECONDS:
                        return part_lower
            return None
        except Exception:
            return None

    def _scan_for_corrupted_files(self) -> None:
        """
        Scan all CSV files for corruption without performing timeframe validation
        """
        corrupted_files = []
        
        print(f"\nScanning {len(self.csv_files)} files for corruption...")
        
        for file in self.csv_files:
            file_path = os.path.join(self.directory, file)
            integrity_check = self._check_file_integrity(file_path)
            
            if not integrity_check['is_valid']:
                corrupted_files.append((file, integrity_check['issues']))
                print(f"\n⚠️ File integrity issues found in {file}:")
                for issue in integrity_check['issues']:
                    print(f"  - {issue}")
        
        if not corrupted_files:
            print("\n✅ No file corruption detected. All files appear valid.")
        else:
            print(f"\n⚠️ Found {len(corrupted_files)} corrupted files out of {len(self.csv_files)} scanned files.")
            
            # Log details for reference
            self.logger.warning(f"Corrupted files summary:")
            for file, issues in corrupted_files:
                self.logger.warning(f"File: {file}")
                for issue in issues:
                    self.logger.warning(f"  - {issue}")
            
            print("\nSuggested repair steps:")
            print("1. For empty or severely corrupted files: redownload the data")
            print("2. For files with NaN values or out-of-order timestamps: run validation with --fill-gaps")
            print("3. For files with minor corruption: try opening in pandas and rewriting with df.to_csv()")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Validate OHLCV data files timestamp spacing')
    parser.add_argument(
        '--directory', '-d',
        type=str,
        default='./csv_ohlcv',
        help='Directory containing CSV files'
    )
    parser.add_argument(
        '--timeframe', '-t',
        type=str,
        choices=['1m', '3m', '5m', '15m', '30m', '1h', '2h', '3h', '4h', '6h', '12h', '1d', '1w'],
        help='Specific timeframe to validate (optional)'
    )
    parser.add_argument(
        '--pair', '-p',
        type=str,
        help='Trading pair to validate (e.g., BTC_USDT) (optional)'
    )
    parser.add_argument(
        '--fill-gaps', '-f',
        action='store_true',
        help='Attempt to fill gaps in the data by downloading missing candles'
    )

    # Add integrity-only flag
    parser.add_argument(
        '--integrity-only', '-i',
        action='store_true',
        help='Only check file integrity without validating timeframes'
    )
    
    args = parser.parse_args()

    print(f"Starting validation of files in {args.directory}")
    if args.timeframe:
        print(f"Filtering for timeframe: {args.timeframe}")
    if args.pair:
        print(f"Filtering for pair: {args.pair}")
    if args.integrity_only:
        print("Performing integrity checks only")

    runner = ValidationRunner(args.directory, args.timeframe, args.pair)
    
    if args.integrity_only:
        # In this case we're only interested in file corruption
        runner._scan_for_corrupted_files()
    else:
        runner.run_validation()

    if args.fill_gaps:
        print("\nAttempting to fill gaps in the data...")
        runner.fill_gaps()
