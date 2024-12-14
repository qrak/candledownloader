from typing import Union, List

import numpy as np


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