# Candle Downloader

## Overview
Candle Downloader is a Python application designed to download OHLCV (Open, High, Low, Close, Volume) data from multiple cryptocurrency exchanges. The application validates the data and fills in gaps where necessary.

## Features
- Downloads historical candlestick data from cryptocurrency exchanges
- Supports multiple timeframes from 1 minute to 1 week
- Data validation and gap detection with optional filling
- Volume-based trading pair selection and ranking
- Smart filtering of stablecoin pairs
- Efficient data buffering and CSV file management
- Comprehensive logging system
- Rate limiting protection and automatic retry mechanism
- Configurable batch size and buffer management
- Flexible output file naming

## Supported Exchanges
- Application tested on binance and binanceus. Other exchanges may work as well.

## Project Structure
- **src/**: Contains the main source code for the application.
  - `candledownloader.py`: Core functionality with `CandleDownloader`, `CandleDataDownloader`, and `ExchangeInterface` classes
  - `config.py`: Configuration settings handler
  - `data_manager.py`: Manages data operations and CSV file handling
  - `logger_manager.py`: Handles logging with customizable formats
  - `timeframe_manager.py`: Manages different timeframes and timestamp calculations
- **utils/**: Utility functions and helper classes
  - Various utility modules for data processing

## Logging System
The application provides a comprehensive logging system:

#### Log Configuration
- **Format**: `YYYY-MM-DD HH:MM:SS message`
- **Destinations**: Console output and optional file logging
- **Log Levels**: INFO, WARNING, ERROR, DEBUG
- **Features**:
  - Download progress tracking
  - Error reporting and stack traces
  - Performance metrics
  - Data validation results
  - Exchange API responses

#### Sample Log Output
```
2023-12-14 20:26:42 Starting download for BTC/USDT (1h)
2023-12-14 20:26:43 Batch 1/100 (1%) - Downloaded 1000 candles
2023-12-14 20:26:44 Batch 2/100 (2%) - Downloaded 1000 candles
2023-12-14 20:26:45 Rate limit exceeded, waiting 60 seconds...
2023-12-14 20:27:45 Resuming download...
```

## Data Management

### CSV File Structure
The application stores data in CSV format with the following features:

#### Columns
- `timestamp`: Unix timestamp in milliseconds (integer)
- `open`: Opening price (float)
- `high`: Highest price in the period (float)
- `low`: Lowest price in the period (float)
- `close`: Closing price (float)
- `volume`: Trading volume (float)

#### Storage Features
- In-memory data buffering with configurable size
- Append-mode writing for continuous updates
- Automatic header management
- Timestamp-based data ordering
- Duplicate prevention
- Data integrity checks
- Efficient sequential writing and random access

## Data Validation

The application includes a separate validation script (`validate.py`) for verifying downloaded data and filling gaps:

### Usage
```bash
python validate.py [-h] [--directory DIRECTORY] [--timeframe {1m,3m,5m,15m,30m,1h,2h,3h,4h,6h,12h,1d,1w}] [--pair PAIR] [--fill-gaps]
```

### Arguments
- `--directory`, `-d`: Directory containing CSV files (default: './csv_ohlcv')
- `--timeframe`, `-t`: Specific timeframe to validate (e.g., '1h', '4h')
- `--pair`, `-p`: Trading pair to validate (e.g., 'BTC_USDT')
- `--fill-gaps`, `-f`: Optional flag to attempt gap filling using other exchanges

### Examples
```bash
# Validate all files in default directory
python validate.py

# Validate specific timeframe and pair
python validate.py --timeframe 1h --pair BTC_USDT

# Validate and fill gaps for specific files
python validate.py --directory ./my_data --timeframe 4h --pair ETH_USDT --fill-gaps
```

### Features
- Validates CSV file format and structure
- Checks data types and timestamp sequences
- Identifies gaps in the data
- Can attempt to fill gaps using data from other exchanges
- Provides detailed validation reports

## Configuration Guide

### Configuration
Before running the application, you need to set up your `config.cfg` file with the following parameters:

```ini
[DEFAULT]
# Exchange Configuration
exchange_name = binance  # Currently, kucoin is not working due to API issues

# Trading Pair Selection
all_pairs = True
base_symbols = FET  # Currently set to FET, but can be changed to other base symbols like BTC, ETH, etc.
quote_symbols = USDT

# Time Configuration
# Comma-separated list of timeframes like 1h, 1d, etc.
timeframes = 5m  # Currently set to 5m, but can be changed to other timeframes
start_time = 2015-01-01T00:00:00Z
end_time = 2022-12-31T23:59:59Z  # Set to a specific end time, but can be left empty for None

# Download Settings
batch_size = 1000
output_directory = ./csv_ohlcv
output_file =  # Leave empty for None

# Logging Configuration
enable_logging = False
```

## Running the Application
To run the application:
```bash
python main.py
```
This will read the configuration from `config.cfg` and download the candle data according to your settings.

## Troubleshooting

### Common Issues
1. **Rate Limit Exceeded**
   - The application will automatically pause and retry after 60 seconds
   - Consider reducing batch_size in config
   - Check exchange API limits

2. **Network Errors**
   - Automatic retry mechanism is in place
   - Check internet connection
   - Verify exchange API availability

3. **Data Gaps**
   - Use validation tools to identify gaps
   - Check exchange maintenance windows
   - Consider cross-exchange validation

4. **Memory Usage**
   - Adjust buffer_size in config
   - Monitor system resources
   - Consider batch processing for large datasets

### Error Messages
- `Invalid pair name`: Verify trading pair exists on exchange
- `Invalid timeframe`: Check supported timeframes for exchange
- `Rate limit exceeded`: Temporary pause, automatic retry
- `Failed to fetch`: Check exchange availability

## Requirements
Ensure you have the necessary dependencies installed:
```bash
pip install -r requirements.txt
```

## Contributing
Feel free to contribute to the project by submitting issues or pull requests.

## License
This project is licensed under the MIT License.
