from typing import List, Optional, Dict
import os
import argparse
from collections import defaultdict
from classdir.candledownloader import TimeframeValidator


class ValidationRunner:
    def __init__(self, directory: str, timeframe: Optional[str] = None, pair: Optional[str] = None):
        self.directory = directory
        self.timeframe = timeframe
        self.pair = pair
        self.csv_files = self._get_csv_files()
        self.validation_results: Dict[str, Dict] = {}

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
                validator = TimeframeValidator(file_path, timeframe)
                result = validator.validate()

                pair_name = self._extract_pair_name(file)
                self.validation_results[file] = {
                    'pair': pair_name,
                    'timeframe': timeframe,
                    'gaps': result['gaps_count'],
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

    args = parser.parse_args()

    print(f"Starting validation of files in {args.directory}")
    if args.timeframe:
        print(f"Filtering for timeframe: {args.timeframe}")
    if args.pair:
        print(f"Filtering for pair: {args.pair}")

    runner = ValidationRunner(args.directory, args.timeframe, args.pair)
    runner.run_validation()
