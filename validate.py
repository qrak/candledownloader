import argparse
import logging
import os
import time
from collections import defaultdict
from datetime import datetime
from typing import List, Optional, Dict, Any

import ccxt
import pandas as pd

from classdir.candledownloader import TimeframeHandler, LoggerManager


class MultiExchangeGapFiller:
    def __init__(self) -> None:
        self.exchanges = {
            # Certified Exchanges (Most Reliable)
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
            'htx': ccxt.htx({'enableRateLimit': True}),  # Former Huobi
            'kucoin': ccxt.kucoin({'enableRateLimit': True}),
            'kucoinfutures': ccxt.kucoinfutures({'enableRateLimit': True}),
            'mexc': ccxt.mexc({'enableRateLimit': True}),
            'okx': ccxt.okx({'enableRateLimit': True}),
            'woo': ccxt.woo({'enableRateLimit': True}),

            # Other Major Exchanges
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
            'poloniexfutures': ccxt.poloniexfutures({'enableRateLimit': True}),
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
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        log_file = os.path.join(
            LoggerManager.LOG_DIR,
            f'validation_runner_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        )

        return LoggerManager.setup_logger(
            f"{__name__}.ValidationRunner",
            True,
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
        parts = filename.split('_')
        return f"{parts[0]}_{parts[1]}"

    def run_validation(self) -> None:
        for file in self.csv_files:
            file_path = os.path.join(self.directory, file)
            timeframe = self._extract_timeframe(file)

            if timeframe:
                print(f"\nValidating {file}...")
                validator = TimeframeHandler(file_path, timeframe)
                result = validator.validate()

                pair_name = self._extract_pair_name(file)
                self.validation_results[file] = {
                    'pair': pair_name,
                    'timeframe': timeframe,
                    'gaps_count': result['gaps_count'],
                    'gaps': result['gaps'],  # Store the full gaps information
                    'invalid_intervals': result['invalid_intervals_count']
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
        # Use all available exchanges
        exchanges = list(self.gap_filler.exchanges.keys())
        file_path = os.path.join(self.directory, file)  # Get full file path
        
        for gap in gaps_list:
            gap_filled = False
            interval_ms = TimeframeHandler.get_timeframe_in_seconds(timeframe) * 1000
            num_candles = (gap['end'] - gap['start']) // interval_ms
            gap_start_year = datetime.fromtimestamp(gap['start'] / 1000).year

            # More lenient coverage threshold for historical gaps
            coverage_threshold = 50 if gap_start_year < 2020 else 80

            self.logger.info(f"\nAttempting to fill gap from {datetime.fromtimestamp(gap['start'] / 1000)} "
                           f"to {datetime.fromtimestamp(gap['end'] / 1000)} "
                           f"(need {num_candles} candles)")

            for exchange_name in exchanges:
                try:
                    exchange = self.gap_filler.exchanges[exchange_name]

                    if timeframe not in exchange.timeframes:
                        continue

                    self.logger.info(f"Trying to fill gap using {exchange_name}")

                    current_start = gap['start']
                    exchange_candles = []

                    # Fetch candles in chunks
                    while current_start < gap['end']:
                        try:
                            # Calculate remaining candles needed
                            remaining_ms = gap['end'] - current_start
                            remaining_candles = remaining_ms // interval_ms
                            chunk_size = min(remaining_candles, 500)  # Use smaller chunks

                            ohlcv = self.gap_filler.fetch_candles(
                                exchange_name,
                                pair,
                                timeframe,
                                current_start,
                                int(chunk_size)
                            )

                            if not ohlcv:
                                self.logger.warning(f"No data returned from {exchange_name}")
                                break

                            # Filter valid candles within gap range
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
                            time.sleep(exchange.rateLimit / 1000 * 2)  # Double the rate limit to be safe

                        except ccxt.RateLimitExceeded:
                            self.logger.warning(f"Rate limit exceeded for {exchange_name}, waiting 30 seconds")
                            time.sleep(30)
                            continue
                        except Exception as e:
                            self.logger.error(f"Error fetching chunk from {exchange_name}: {str(e)}")
                            break

                    if exchange_candles:
                        # Process candles from this exchange
                        df = pd.DataFrame(
                            exchange_candles,
                            columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
                        )
                        df = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp')

                        # Verify gap coverage without any interpolation
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
                            # Update the CSV file with new data
                            self._update_csv_with_gap_data(file_path, df.values.tolist())
                            gap_filled = True
                            self.logger.info(f"Successfully filled gap with {coverage:.2f}% coverage using {exchange_name}")
                            break  # Stop trying other exchanges

                except Exception as e:
                    self.logger.error(f"Error with exchange {exchange_name}: {str(e)}")
                    continue

            if not gap_filled:
                self.logger.warning("Failed to fill gap with any exchange")

    def _update_csv_with_gap_data(self, file_path: str, new_data: List[List[Any]]) -> None:
        df = pd.read_csv(file_path)
        new_df = pd.DataFrame(
            new_data,
            columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
        )

        combined_df = pd.concat([df, new_df])
        combined_df = combined_df.drop_duplicates(subset=['timestamp'])
        combined_df = combined_df.sort_values('timestamp')

        combined_df.to_csv(file_path, index=False)

    @staticmethod
    def _extract_timeframe(filename: str) -> Optional[str]:
        try:
            parts = filename.split('_')
            for part in parts:
                if part.endswith(('m', 'h', 'd', 'w')):
                    return part
            return None
        except Exception:
            return None


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

    args = parser.parse_args()

    print(f"Starting validation of files in {args.directory}")
    if args.timeframe:
        print(f"Filtering for timeframe: {args.timeframe}")
    if args.pair:
        print(f"Filtering for pair: {args.pair}")

    runner = ValidationRunner(args.directory, args.timeframe, args.pair)
    runner.run_validation()

    if args.fill_gaps:
        print("\nAttempting to fill gaps in the data...")
        runner.fill_gaps()
