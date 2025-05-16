import ccxt
from typing import List

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

    def fetch_ohlcv(self, symbol: str, timeframe: str, since: int, limit: int):
        return self.exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)
