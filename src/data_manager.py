import os
from typing import List, Any

import pandas as pd


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
