# Candle Downloader

## Overview
Candle Downloader is a Python application designed to download OHLCV (Open, High, Low, Close, Volume) data from multiple cryptocurrency exchanges. The application features advanced validation capabilities, integrity checks, gap detection and filling, with an elegant terminal interface for monitoring download progress.

## Features
- Downloads historical candlestick data from cryptocurrency exchanges
- Supports multiple timeframes from 1 minute to 1 week
- Clean terminal interface with progress bars and status updates
- Data validation with integrity checks and gap detection
- Automatic handling of file corruption from interrupted downloads
- Volume-based trading pair selection and ranking
- Smart filtering of stablecoin pairs
- Command-line arguments for flexible operation
- Efficient data buffering and CSV file management
- Comprehensive logging system
- Rate limiting protection with exponential backoff retry mechanism
- Configurable batch size and buffer management
- Flexible output file naming

## Supported Exchanges
- Application tested on binance and binanceus. Other exchanges may work as well.
- Multi-exchange gap filling supports 30+ exchanges for data validation and repair.

## Project Structure
- **src/**: Contains the main source code for the application.
  - `candledownloader.py`: Core functionality with `CandleDownloader`, `CandleDataDownloader`, and `ExchangeInterface` classes
  - `config.py`: Configuration settings handler
  - `data_manager.py`: Manages data operations and CSV file handling
  - `logger_manager.py`: Handles logging with customizable formats
  - `timeframe_manager.py`: Manages different timeframes and timestamp calculations
- **utils/**: Utility functions and helper classes
  - `average_quote_vol.py`: Volume calculations for ranking trading pairs
- **Main scripts**:
  - `main.py`: Primary script for downloading data
  - `validate.py`: Tool for validating and repairing data files

## Command-Line Usage

### Main Application
```bash
python main.py [--config CONFIG] [--pairs PAIRS] [--timeframes TIMEFRAMES] [--most-traded] [--days DAYS] [--limit LIMIT]
```

#### Arguments
- `--config`, `-c`: Path to configuration file (default: config.cfg)
- `--pairs`, `-p`: Comma-separated list of trading pairs (overrides config)
- `--timeframes`, `-t`: Comma-separated list of timeframes (overrides config)
- `--most-traded`, `-m`: Download most traded pairs by volume
- `--days`, `-d`: Days to look back for most traded pairs (default: 365)
- `--limit`, `-l`: Limit number of pairs for most traded (default: 100)

### Validation Tool
```bash
python validate.py [--directory DIR] [--timeframe TIMEFRAME] [--pair PAIR] [--fill-gaps] [--integrity-only]
```

#### Arguments
- `--directory`, `-d`: Directory containing CSV files (default: './csv_ohlcv')
- `--timeframe`, `-t`: Specific timeframe to validate (e.g., '1h', '4h')
- `--pair`, `-p`: Trading pair to validate (e.g., 'BTC_USDT')
- `--fill-gaps`, `-f`: Attempt to fill gaps using data from other exchanges
- `--integrity-only`, `-i`: Only check file integrity without validating timeframes

### Examples
```bash
# Download using config settings
python main.py

# Download specific pairs and timeframes
python main.py --pairs BTC,ETH --timeframes 1h,4h

# Download most traded pairs
python main.py --most-traded --limit 50

# Validate all files in default directory
python validate.py

# Check only file integrity (corruption)
python validate.py --integrity-only

# Validate and fill gaps for specific files
python validate.py --directory ./my_data --timeframe 4h --pair ETH_USDT --fill-gaps
```

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

## Data Validation and Integrity

The application includes robust validation capabilities:

### Timeframe Validation
- Ensures timestamps are correctly spaced according to timeframe
- Identifies gaps in the data sequence
- Verifies that candle intervals match the expected timeframe

### File Integrity Checks
- Detects corruption caused by interrupted downloads (CTRL+C)
- Validates CSV file structure and format
- Checks for missing or empty files
- Identifies NaN values in data (possible truncation)
- Verifies data consistency (high >= low, close within range)
- Checks timestamp ordering
- Examines file endings for proper completion

### Gap Filling
- Cross-exchange verification and data retrieval
- Smart pair name matching across different exchanges
- Coverage percentage calculation for gap filling quality
- Adaptive threshold based on data age

## Configuration Guide

### Configuration
Before running the application, you need to set up your `config.cfg` file with the following parameters:

```ini
[DEFAULT]
# Exchange Configuration
exchange_name = binance  # Currently, kucoin is not working due to API issues

# Trading Pair Selection
all_pairs = True  # Set to True to download all available trading pairs
base_symbols = BTC,ETH,BNB  # Example symbols, used when all_pairs=False
quote_symbols = USDT

# Time Configuration
timeframes = 5m  # Comma-separated list of timeframes
start_time = 2015-01-01T00:00:00Z
end_time =  # Leave empty for current time

# Download Settings
batch_size = 1000
output_directory = ./csv_ohlcv
output_file =  # Leave empty for automatic filename generation

# Logging Configuration
enable_logging = False
```

## Troubleshooting

### Common Issues
1. **Rate Limit Exceeded**
   - The application will automatically pause and retry with exponential backoff
   - Consider reducing batch_size in config
   - Check exchange API limits

2. **Network Errors**
   - Automatic retry mechanism is in place
   - Check internet connection
   - Verify exchange API availability

3. **Data Gaps**
   - Use validation tools to identify gaps
   - Run with `--fill-gaps` to attempt repair
   - Check exchange maintenance windows

4. **File Corruption**
   - Use `python validate.py --integrity-only` to check for corrupted files
   - Follow the suggested repair steps for each type of corruption
   - For severe corruption, redownload the affected data

5. **Memory Usage**
   - Adjust buffer_size in config
   - Monitor system resources
   - Consider batch processing for large datasets

### Error Messages
- `Invalid pair name`: Verify trading pair exists on exchange
- `Invalid timeframe`: Check supported timeframes for exchange
- `Rate limit exceeded`: Temporary pause, automatic retry
- `Failed to fetch`: Check exchange availability
- `File integrity issues`: Run validation with integrity check

## Requirements
Ensure you have the necessary dependencies installed:
```bash
pip install -r requirements.txt
```

## Contributing
Feel free to contribute to the project by submitting issues or pull requests.

## License
This project is licensed under the MIT License.
