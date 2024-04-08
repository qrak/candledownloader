from classdir.candledownloader import CandleDataDownloader
from configparser import ConfigParser

if __name__ == "__main__":

    # Read configurations from config.cfg
    config = ConfigParser()
    config.read('config.cfg')

    exchange_name = config.get('DEFAULT', 'exchange_name')
    all_pairs = config.getboolean('DEFAULT', 'all_pairs')
    base_symbols = config.get('DEFAULT', 'base_symbols').split(',')
    quote_symbols = config.get('DEFAULT', 'quote_symbols').split(',')
    timeframes = config.get('DEFAULT', 'timeframes').split(',')
    start_time = config.get('DEFAULT', 'start_time')
    end_time = config.get('DEFAULT', 'end_time') or None  # It will be None if not specified
    batch_size = config.getint('DEFAULT', 'batch_size')
    output_directory = config.get('DEFAULT', 'output_directory')
    output_file = config.get('DEFAULT', 'output_file') or None  # It will be None if not specified
    enable_logging = config.getboolean('DEFAULT', 'enable_logging')

    # Instantiate CandleDataDownloader class
    downloader = CandleDataDownloader(exchange_name=exchange_name,
                                      all_pairs=all_pairs,
                                      base_symbols=base_symbols,
                                      quote_symbols=quote_symbols,
                                      timeframes=timeframes,
                                      start_time=start_time,
                                      end_time=end_time,
                                      batch_size=batch_size,
                                      output_directory=output_directory,
                                      output_file=output_file,
                                      log_to_file=enable_logging)

    downloader.download_candles_for_pairs(most_traded=False, days=365, limit=100)
