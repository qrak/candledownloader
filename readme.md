# Candlestick Data Downloader

## Overview

This Python script downloads historical candlestick data from various cryptocurrency exchanges, including but not limited to Binance, Bitfinex, BitMEX, Bitstamp, Coinbase Pro, Kraken, and OKEx. It utilizes the `CandleDataDownloader` class to efficiently and flexibly fetch candlestick data. The data is saved into CSV files, making it easy to analyze and visualize. If the program is terminated, the progress is saved. Upon restart, the downloader will continue fetching data where it left off.

## Table of Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
  - [Configuration](#configuration)
  - [Running the Script](#running-the-script)
- [Output Format](#output-format)
- [Dependencies](#dependencies)
- [Logging](#logging)
- [License](#license)

## Requirements

- Python 3.8+
- `ccxt` library
- `pandas` library

## Installation

1. Clone the repository to your local machine: `git clone https://github.com/qrak/candledownloader.git`.
2. Navigate to the `candledownloader` directory.
3. Create a new virtual environment: `python -m venv env`.
4. Activate the virtual environment:
   - On macOS and Linux: `source env/bin/activate`
   - On Windows: `env\Scripts\activate`
5. Install the required packages: `pip install -r requirements.txt`.

## Usage

### Configuration

1. Open the main script (`main.py` or your specified entry file).
2. Set `all_pairs` to `True` for downloading data for all trading pairs, or `False` for specific pairs.
3. Modify the `base_symbols` list to include the desired base currencies if `all_pairs` is set to `False`.
4. Optionally, adjust the `quote_symbols` and `timeframes` lists.
5. Toggle `enable_logging` to `True` for logging.

### Running the Script

Run the script using:

```bash
python main.py
```

### Advanced Options

- Specify the `start_time` if you wish to download historical data from a particular date.
- Progress updates are displayed in the console during data download.

## Output Format

The script outputs data to CSV files in the following column format:

- `timestamp`
- `open`
- `high`
- `low`
- `close`
- `volume`

## Dependencies

- `ccxt`: Library for cryptocurrency trading.
- `pandas`: Data manipulation and analysis.
- `logging`: Standard Python logging library.

## Logging

Enable logging by setting the `enable_logging` variable to `True` in the main script. Logs track the downloading process.

## License

This project is licensed under the MIT License.
