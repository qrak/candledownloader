from datetime import datetime, timedelta
from typing import List, Dict, Optional
import numpy as np
from src.exchange_interface import ExchangeInterface
from src.config import Config
from utils.average_quote_vol import AverageVolumeCalculator

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

        from src.candledownloader import CandleDownloader  # Local import to avoid circular import
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
