import os
from typing import List, Any, Dict
import pandas as pd
import sys


class DataManager:
    def __init__(self, output_file: str, quiet: bool = False):
        self.output_file = output_file
        self.data_buffer: List[Any] = []
        self.quiet = quiet
        self._ensure_directory_exists()

    def _ensure_directory_exists(self) -> None:
        output_dir = os.path.dirname(self.output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
            if not self.quiet:
                print(f"Created output directory: {output_dir}")

    def get_last_timestamp(self) -> int:
        try:
            if not os.path.exists(self.output_file) or os.path.getsize(self.output_file) == 0:
                return 0

            df = pd.read_csv(self.output_file)
            if df.empty or 'timestamp' not in df.columns:
                return 0
            return int(df['timestamp'].iloc[-1])
        except (FileNotFoundError, pd.errors.EmptyDataError, pd.errors.ParserError) as e:
            print(f"Error reading existing file {self.output_file}: {str(e)}")
            return 0
        except Exception as e:
            print(f"Unexpected error reading {self.output_file}: {str(e)}")
            return 0

    def write_buffer(self) -> None:
        if not self.data_buffer:
            return

        try:
            df = pd.DataFrame(self.data_buffer,
                              columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            df = df.sort_values('timestamp')
            df = df.drop_duplicates(subset=['timestamp'], keep='last')

            file_exists = os.path.exists(self.output_file) and os.path.getsize(self.output_file) > 0

            df.to_csv(self.output_file, mode='a', index=False, header=not file_exists)
            
            # Only print if not in quiet mode
            if not self.quiet:
                print(f"Wrote {len(df)} records to {self.output_file}")
                
            self.data_buffer.clear()
        except Exception as e:
            # Even in quiet mode, we should print errors
            sys.stdout.write("\r\033[K")  # Clear the current line
            print(f"Error writing to {self.output_file}: {str(e)}")
