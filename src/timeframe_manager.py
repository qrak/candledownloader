from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any
import pandas as pd


class TimeframeManager:
    TIMEFRAME_TO_SECONDS: Dict[str, int] = {
        "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
        "1h": 3600, "2h": 7200, "3h": 10800, "4h": 14400,
        "6h": 21600, "12h": 43200, "1d": 86400, "1w": 604800,
    }

    @staticmethod
    def get_timeframe_in_seconds(timeframe: str) -> int:
        return TimeframeManager.TIMEFRAME_TO_SECONDS[timeframe]

    @staticmethod
    def get_current_timestamp(timeframe: str) -> int:
        now = datetime.now(timezone.utc)
        if timeframe == '1w':
            start_of_week = now - timedelta(days=now.weekday())
            return int(start_of_week.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
        elif timeframe == '1d':
            return int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
        else:
            timeframe_seconds = TimeframeManager.TIMEFRAME_TO_SECONDS[timeframe]
            current_timestamp = int(now.timestamp() * 1000)
            return current_timestamp - (current_timestamp % (timeframe_seconds * 1000))

    def __init__(self, csv_file: str, timeframe: str):
        self.csv_file = csv_file
        self.timeframe = timeframe
        self.expected_interval = self.TIMEFRAME_TO_SECONDS[timeframe] * 1000
        self.df = pd.read_csv(csv_file)

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
        # Replace logging with print statements
        print(f"Validation results for {self.csv_file}:")
        print(f"Timeframe: {self.timeframe}")
        print(f"Total records: {result['total_records']}")
        print(f"Gaps found: {result['gaps_count']}")
        print(f"Invalid intervals: {result['invalid_intervals_count']}")

        if result['gaps']:
            print("Gaps detected:")
            for gap in result['gaps'][:5]:  # Show just first 5 to avoid console flood
                start_time = datetime.fromtimestamp(gap['start'] / 1000)
                end_time = datetime.fromtimestamp(gap['end'] / 1000)
                gap_td = timedelta(milliseconds=gap['gap_size'])
                print(f"Gap from {start_time} to {end_time} (size: {gap_td})")
            
            if len(result['gaps']) > 5:
                print(f"... and {len(result['gaps']) - 5} more gaps")

        if result['invalid_intervals']:
            print("Invalid intervals detected:")
            for interval in result['invalid_intervals'][:5]:  # Show just first 5
                time = datetime.fromtimestamp(interval['timestamp'] / 1000)
                interval_td = timedelta(milliseconds=interval['interval'])
                print(f"Position {interval['position']}: {time} (interval: {interval_td})")
                
            if len(result['invalid_intervals']) > 5:
                print(f"... and {len(result['invalid_intervals']) - 5} more invalid intervals")

        print(f"Validation {'passed' if result['is_valid'] else 'failed'}")