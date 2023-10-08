# Candlestick Data Downloader

## Overview

This Python script downloads historical candlestick data from various cryptocurrency exchanges. It uses the `CandleDataDownloader` class for efficient and flexible data fetching. The script now supports reading configuration from a `config.cfg` file for easier customization. The data is saved into CSV files, making it easy for subsequent analysis and visualization. If the program is terminated, the progress is saved and can be resumed upon restart.

## Table of Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
  - [Configuration](#configuration)
  - [Advanced Configuration](#advanced-configuration)
  - [Running the Script](#running-the-script)
- [Output Format](#output-format)
- [Dependencies](#dependencies)
- [License](#license)

## Requirements

- Python 3.8+
- `ccxt` library
- `pandas` library
- `ConfigParser` library

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

Edit the `config.cfg` file for basic configuration settings:

- `all_pairs`: Set to `True` for downloading data for all trading pairs, or `False` for specific pairs.
- `base_symbols`: Modify this comma-separated list to include the desired base currencies if `all_pairs` is set to `False`.
- `quote_symbols`: Comma-separated list for quote currencies.
- `timeframes`: Comma-separated list for different timeframes.
- `enable_logging`: Toggle to `True` for logging.

### Advanced Configuration

Advanced options can also be set via the `config.cfg` file, all variables are optional:

- `start_time`: Specify the start time in ISO 8601 format for historical data download.
- `end_time`: Specify the end time in ISO 8601 format for historical data download.
- `batch_size`: Number of records fetched in each request.
- `output_directory`: Directory to save CSV files.
- `output_file`: Optionally, specify an output file name.
- `enable_logging`: Enable output logging to file

### Running the Script

Run the script using:

```bash
python main.py
```

## Output Format

The script outputs data into CSV files with the following columns:

- `timestamp`
- `open`
- `high`
- `low`
- `close`
- `volume`

## Dependencies

- `ccxt`: Library for cryptocurrency trading.
- `pandas`: Data manipulation and analysis.
- `ConfigParser`: Configuration parsing library.
- `logging`: Standard Python logging library.

## License

This project is licensed under the MIT License.
