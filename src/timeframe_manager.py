from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any
import pandas as pd

from src.logger_manager import LoggerManager
from src.config import Config


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
        self.config = Config()
        self.logger = LoggerManager.setup_logger(
            f"{__name__}.TimeframeHandler",
            self.config.log_to_file,
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