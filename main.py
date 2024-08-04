from typing import List, Optional
from classdir.candledownloader import CandleDataDownloader
from configparser import ConfigParser


def load_config(config_file: str = 'config.cfg') -> ConfigParser:
    """
    Load and parse the configuration file.

    Parameters:
    config_file (str): Path to the configuration file. Default is 'config.cfg'.

    Returns:
    ConfigParser: Parsed configuration object.

    Raises:
    FileNotFoundError: If the configuration file is not found.
    configparser.Error: If there's an error parsing the configuration file.
    """
    config = ConfigParser()
    config.read(config_file)
    return config


def create_downloader(cfg: ConfigParser) -> CandleDataDownloader:
    """
    Create a CandleDataDownloader instance based on the provided configuration.

    Parameters:
    cfg (ConfigParser): Parsed configuration object.

    Returns:
    CandleDataDownloader: Configured instance of CandleDataDownloader.

    Raises:
    ValueError: If required configuration values are missing or invalid.
    """
    try:
        return CandleDataDownloader(cfg)
    except KeyError as e:
        raise ValueError(f"Missing required configuration value: {e}")
    except ValueError as e:
        raise ValueError(f"Invalid configuration value: {e}")


if __name__ == "__main__":
    """
    Main entry point for the candle data downloader script.

    This script reads configuration from a file, creates a CandleDataDownloader instance,
    and initiates the download process for the specified trading pairs and timeframes.

    Configuration file (config.cfg) should contain the following fields:
    - exchange_name: Name of the exchange (e.g., 'binance')
    - all_pairs: Boolean flag to download data for all available pairs
    - base_symbols: Comma-separated list of base symbols (e.g., 'BTC,ETH')
    - quote_symbols: Comma-separated list of quote symbols (e.g., 'USDT,USD')
    - timeframes: Comma-separated list of timeframes (e.g., '1h,1d,1w')
    - start_time: Start time for data download in ISO 8601 format (default will start from 2015 which is enough 
    for most exchanges)
    - end_time: End time for data download in ISO 8601 format (optional)
    - batch_size: Number of candles to fetch in each API request
    - output_directory: Directory to save the downloaded data
    - output_file: Specific filename for the output (optional)
    - enable_logging: Boolean flag to enable logging to a file

    Raises:
    FileNotFoundError: If the configuration file is not found.
    configparser.Error: If there's an error parsing the configuration file.
    ValueError: If required configuration values are missing or invalid.
    """
    config = load_config()
    downloader = create_downloader(config)
    downloader.download_candles_for_pairs()
