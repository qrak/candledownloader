import ccxt
import pandas as pd
import time
import os


class CandleDownloader:
    def __init__(self, exchange_name='binance', pair_name='BTC/USDT', timeframe='5m',
                 start_time='2015-01-01T00:00:00Z', end_time=None, batch_size=1000,
                 output_directory='./csv_ohlcv', output_file=None):
        self.exchange = getattr(ccxt, exchange_name)(
            {
                "enableRateLimit": True,
            })
        self.pair_name = pair_name
        self.timeframe = timeframe
        self.start_time = start_time
        self.end_time = end_time
        self.batch_size = batch_size
        self.output_directory = output_directory
        self.output_file = output_file
        self.total_candles = 0
        self.total_batches = 0
        self.buffer = []

        # Validate the parameters
        if pair_name not in self.exchange.load_markets():
            raise ValueError(f"Invalid pair name: {pair_name}")
        if timeframe not in self.exchange.timeframes:
            raise ValueError(f"Invalid timeframe: {timeframe}")

        # Set the filename and path for the output file
        if output_file is None:
            self.output_file = self.generate_output_filename()

    def generate_output_filename(self):
        os.makedirs(self.output_directory, exist_ok=True)
        symbol_base = self.pair_name.split('/')[0]
        symbol_quote = self.pair_name.split('/')[1]
        start_date = self.start_time.split('T')[0]
        end_date = self.end_time.split('T')[0] if self.end_time else 'now'
        filename = f'{symbol_base}_{symbol_quote}_{self.timeframe}_{start_date}_{end_date}_{self.exchange.id}.csv'
        return os.path.join(self.output_directory, filename)

    def write_to_output_file(self, df):
        if os.path.isfile(self.output_file):
            with open(self.output_file, mode='a', newline='') as f:
                if f.tell() == 0:
                    df.to_csv(f, index=False, header=True)
                else:
                    df.to_csv(f, index=False, header=False)
        else:
            df.to_csv(self.output_file, index=False, header=True)

    def download_candles(self):
        # Check if the file already exists
        try:
            df = pd.read_csv(self.output_file, usecols=[0], header=None, skiprows=1)
            self.start_time = int(df.iloc[-1, 0]) + (
                    self.exchange.parse_timeframe(self.timeframe) * 1000)
            print(f"Resuming from timestamp {self.start_time}...")
        except (FileNotFoundError, pd.errors.EmptyDataError):
            self.start_time = self.exchange.parse8601(self.start_time)

        # Fetch the candles and write them to the output file
        while True:
            try:
                # Fetch the candles for the current time range
                candles = self.exchange.fetch_ohlcv(self.pair_name, self.timeframe, since=self.start_time,
                                                    limit=self.batch_size)

                # Convert the fetched candles to a pandas dataframe
                df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

                if len(candles) == 0:
                    break

                # Append the dataframe to the in-memory buffer
                self.buffer.append(df)

                # Update the start time for the next request
                self.start_time = candles[-1][0] + self.exchange.parse_timeframe(self.timeframe) * 1000

                # Update progress message
                self.total_candles += len(df)
                self.total_batches += 1
                print(f"Downloaded {self.total_candles} candles for {self.pair_name}, timeframe: {self.timeframe} in {self.total_batches} batches...")

                # Write the accumulated data to the output file
                self.write_to_output_file(df)

                # Clear the buffer
                self.buffer = []

            except (ccxt.RateLimitExceeded, ccxt.DDoSProtection) as e:
                print(f"Rate limit exceeded: {e}. Retrying in 60 seconds...")
                time.sleep(60)
            except ccxt.BaseError as e:
                print(f"Exception occurred: {e}. Retrying in 60 seconds...")
                time.sleep(60)

                # Write the remaining data in the buffer to the output file
                if len(self.buffer) > 0:
                    df = pd.concat(self.buffer)
                    self.write_to_output_file(df)
                    self.buffer = []

        # Print a message when the script has finished
        print(
            f'Download complete. Total candles: {self.total_candles}, Total batches: {self.total_batches}, Output file: {self.output_file}')