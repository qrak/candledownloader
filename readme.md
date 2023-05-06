# Cryptocurrency Candle Downloader

This Python script downloads historical candle data from a cryptocurrency exchange and saves it to a CSV file. It uses the CCXT library to interact with various cryptocurrency exchanges, including Binance, Bitfinex, BitMEX, Bitstamp, Coinbase Pro, Kraken, and OKEx.

## Installation

1. Clone the `candledownloader` repository to your local machine using `git clone https://github.com/qrak/candledownloader.git`.
2. Navigate to the `candledownloader` directory.
3. Create a new virtual environment for this project using `python -m venv env`.
4. Activate the virtual environment using `source env/bin/activate`.
5. Install the required Python packages using `pip install -r requirements.txt`.

## Usage

To use the script, modify the `base_symbols` and `quote_symbols` lists in `main.py` to include the trading pairs you want to download data for.

For example, to download 1-hour candles for the BTC/USDT, ETH/USDT, and ADA/USDT pairs on Binance from January 1, 2015 to April 1, 2022 and save the output to CSV files, modify the `base_symbols` and `quote_symbols` lists as follows:

```
base_symbols = ['BTC', 'ETH', 'ADA']
quote_symbols = ['USDT']
```

Then, run the `main.py` file in your Python editor or IDE.

Note that some exchanges may not have data available for dates that far back. In that case, the script will download as much data as possible and skip any missing data.

If you want to download all available historical data for a trading pair, modify the `start_time` argument to the oldest date available on the exchange. For example, on Binance, the oldest available data is from August 17, 2017, so you could use `start_time='2017-08-17T00:00:00Z'` to download all available historical data.

If you don't know exact date when trading started on the exchange, you could use for example `start_time='2010-01-01T00:00:00Z'`

The script will output progress updates in the console as it downloads the candle data. The final output will be saved to a CSV file in the specified output directory with the specified filename.

## Output Format

The script saves the candle data to a CSV file in the following format:

| timestamp  | open     | high     | low      | close    | volume    |
|------------|----------|----------|----------|----------|-----------|
| 1609459200 | 29301.95 | 29387.62 | 29249.05 | 29288.98 | 3101.4438 |
| 1609462800 | 29288.98 | 29335.69 | 29205.14 | 29260.00 | 2892.0012 |
| 1609466400 | 29260.01 | 29329.85 | 29215.48 | 29299.99 | 2897.4134 |
| ...        | ...      | ...      | ...      | ...      | ...       |

The columns represent the timestamp of each candle (in Unix format), the opening price, the highest price, the lowest price, the closing price, and the volume of the asset traded during the candle period. The timestamp can be converted to a human-readable date and time using a Unix timestamp converter.

## License

This project is licensed under the MIT License.