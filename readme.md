# Cryptocurrency Candle Downloader

This Python script downloads historical candle data from a cryptocurrency exchange and saves it to a CSV file.

## Installation

1. Clone this repository to your local machine.
2. Create a new virtual environment for this project using `python -m venv env`.
3. Activate the virtual environment using `source env/bin/activate`.
4. Install the required Python packages using `pip install -r requirements.txt`.

## Usage

To use the script, simply run the `candledownloader.py` file in the command line and provide the following arguments:

- `--exchange`: the name of the cryptocurrency exchange (default: "binance")
- `--pair`: the name of the trading pair (default: "BTC/USDT")
- `--timeframe`: the candle timeframe (default: "1m")
- `--start_time`: the start time of the data to download (default: "2015-01-01T00:00:00Z")
- `--end_time`: the end time of the data to download (default: To current time)
- `--batch_size`: the number of candles to download in each API request (default: 1000)
- `--output_directory`: the directory to save the output CSV file (default: "./csv_ohlcv")
- `--output_file`: the name of the output CSV file (default: generated based on other parameters)

For example, to download 1-hour candles for the BTC/USDT pair on Binance from January 1, 2021 to April 1, 2021 and save the output to a file called `btc_usdt_1h_2021-01-01_2021-04-01_binance.csv`, run the following command:

python candledownloader.py --pair BTC/USDT --timeframe 1h --start_time 2021-01-01T00:00:00Z --end_time 2021-04-01T00:00:00Z


## Output Format

The script saves the candle data to a CSV file in the following format:

| timestamp  | open     | high     | low      | close    | volume    |
|------------|----------|----------|----------|----------|-----------|
| 1609459200 | 29301.95 | 29387.62 | 29249.05 | 29288.98 | 3101.4438 |
| 1609462800 | 29288.98 | 29335.69 | 29205.14 | 29260.00 | 2892.0012 |
| 1609466400 | 29260.01 | 29329.85 | 29215.48 | 29299.99 | 2897.4134 |
| ...        | ...      | ...      | ...      | ...      | ...       |

The columns represent the timestamp of each candle (in Unix format), the opening price, the highest price, the lowest price, the closing price, and the volume of the asset traded during the candle period.

## Supported Exchanges

The script uses the CCXT library to interact with cryptocurrency exchanges. Currently, the following exchanges are supported:

- Binance (tested, working)
- Bitfinex (not tested)
- BitMEX (not tested)
- Bitstamp (not tested)
- Coinbase Pro (not tested)
- Kraken (not tested)
- OKEx (not tested)

