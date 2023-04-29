# Cryptocurrency Candle Downloader

This Python script downloads historical candle data from a cryptocurrency exchange and saves it to a CSV file. It uses the CCXT library to interact with various cryptocurrency exchanges, including Binance, Bitfinex, BitMEX, Bitstamp, Coinbase Pro, Kraken, and OKEx.

## Installation

1. Clone the `candledownloader` repository to your local machine using `git clone https://github.com/<username>/candledownloader.git`.
2. Navigate to the `candledownloader` directory.
3. Create a new virtual environment for this project using `python -m venv env`.
4. Activate the virtual environment using `source env/bin/activate`.
5. Install the required Python packages using `pip install -r requirements.txt`.

## Usage

To use the script, simply run the `candledownloader.py` file in the command line and provide the desired arguments (see the script comments for more details on each argument).

For example, to download 1-hour candles for the BTC/USDT pair on Binance from January 1, 2015 to April 1, 2022 and save the output to a file called `btc_usdt_1h_2015-01-01_2022-04-01_binance.csv`, run the following command:

```
python candledownloader.py --pair BTC/USDT --timeframe 1h --start_time 2015-01-01T00:00:00Z --end_time 2022-04-01T00:00:00Z
```

Note that some exchanges may not have data available for dates that far back. In that case, the script will download as much data as possible and skip any missing data.

If you want to download all available historical data for a trading pair, simply set the `start_time` argument to the oldest date available on the exchange. For example, on Binance, the oldest available data is from August 17, 2017, so you could use `--start_time 2017-08-17T00:00:00Z` to download all available historical data.

The script will output progress updates in the command line as it downloads the candle data. The final output will be saved to a CSV file in the specified output directory with the specified filename.

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