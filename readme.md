# Candle Downloader

## Overview
Candle Downloader is a Python application designed to download OHLCV (Open, High, Low, Close, Volume) data from multiple cryptocurrency exchanges. The application validates the data and fills in gaps where necessary.

## Features
- Downloads historical candlestick data from various cryptocurrency exchanges
- Supports multiple timeframes from 1 minute to 1 week
- Automatic gap detection and filling
- Efficient data buffering and CSV file management
- Comprehensive logging system
- Data validation and verification tools

## Project Structure
- **src/**: Contains the main source code for the application.
  - `candledownloader.py`: Main functionality for downloading candle data.
  - `config.py`: Configuration settings handler.
  - `data_manager.py`: Manages data operations and CSV file handling.
  - `logger_manager.py`: Handles logging with customizable formats.
  - `timeframe_manager.py`: Manages different timeframes and timestamp calculations.

## Supported Timeframes
The application supports the following timeframes:
- **Minutes**: 1m, 3m, 5m, 15m, 30m
- **Hours**: 1h, 2h, 3h, 4h, 6h, 12h
- **Days/Weeks**: 1d, 1w

## Usage

### Configuration
Before running the application, you need to set up your `config.cfg` file with the following parameters:

```ini
[DEFAULT]
# Name of the exchange (e.g., binance)
exchange_name = binance

# Set to True to download data for all trading pairs, False for specific pairs
all_pairs = True

# If all_pairs = False, specify the trading pairs (comma-separated)
base_symbols = BTC,ETH
quote_symbols = USDT

# Timeframes for the candle data (comma-separated)
timeframes = 1h,4h

# Start time for data collection (ISO 8601 format)
start_time = 2023-01-01T00:00:00Z

# Optional: End time for data collection
end_time = 2023-12-31T23:59:59Z

# Optional: Number of records per request
batch_size = 1000

# Optional: Directory for saving CSV files
output_directory = ./csv_ohlcv

# Optional: Specific output filename
output_file = 

# Optional: Enable logging to file
log_to_file = False
```

### Running the Application
To run the application:
```bash
python main.py
```
This will read the configuration from `config.cfg` and download the candle data according to your settings.

### Output Format
The downloaded data is saved in CSV format with the following columns:
- `timestamp`: Unix timestamp in milliseconds
- `open`: Opening price
- `high`: Highest price
- `low`: Lowest price
- `close`: Closing price
- `volume`: Trading volume

### Data Validation
To validate the downloaded OHLCV data files, use `validate.py`:
```bash
python validate.py --directory <path_to_directory> --timeframe <timeframe> --pair <currency_pair>
```

#### Validation Arguments
- `--directory` or `-d`: Directory containing CSV files (default: './csv_ohlcv')
- `--timeframe` or `-t`: Specific timeframe to validate
- `--pair` or `-p`: Trading pair to validate (e.g., BTC_USDT)
- `--fill-gaps` or `-f`: Attempt to fill gaps in the data

## Logging
The application includes a comprehensive logging system:
- Logs are stored in the `logs` directory
- Default format: `YYYY-MM-DD HH:MM:SS message`
- Configurable log levels and formats
- Separate log files for different components

## Requirements
Ensure you have the necessary dependencies installed:
```bash
pip install -r requirements.txt
```

## Contributing
Feel free to contribute to the project by submitting issues or pull requests.

## License
This project is licensed under the MIT License.
