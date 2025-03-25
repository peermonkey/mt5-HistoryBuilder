import os
import logging
from datetime import datetime, timedelta
import pandas as pd
import MetaTrader5 as mt5
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
import pytz

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MT5DataExtractor:
    def __init__(self):
        # Load environment variables
        load_dotenv()

        # MT5 Connection Parameters
        self.login = int(os.getenv('MT5_LOGIN'))
        self.password = os.getenv('MT5_PASSWORD')
        self.server = os.getenv('MT5_SERVER')
        
        # MongoDB Connection Parameters
        self.mongo_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
        self.mongo_db = os.getenv('MONGODB_DATABASE', 'trading_data')

        # Data Extraction Parameters
        self.symbols = os.getenv('TRADING_SYMBOLS', '').split(',')
        self.timeframe_mapping = {
            '1M': mt5.TIMEFRAME_M1,
            '5M': mt5.TIMEFRAME_M5,
            '15M': mt5.TIMEFRAME_M15,
            '30M': mt5.TIMEFRAME_M30,
            '1H': mt5.TIMEFRAME_H1,
            '4H': mt5.TIMEFRAME_H4,
            '1D': mt5.TIMEFRAME_D1
        }
        self.selected_timeframes = os.getenv('TIMEFRAMES', '1D').split(',')
        
        # MongoDB Client
        self.mongo_client = MongoClient(self.mongo_uri)
        self.db = self.mongo_client[self.mongo_db]

    def connect_mt5(self):
        """
        Establish connection to MT5 platform
        """
        if not mt5.initialize(login=self.login, 
                               password=self.password, 
                               server=self.server):
            logger.error("MT5 Initialization Failed")
            return False
        return True

    def fetch_historical_data(self, symbol, timeframe):
        """
        Fetch comprehensive historical price data for a given symbol and timeframe
        """
        mt5_timeframe = self.timeframe_mapping.get(timeframe, mt5.TIMEFRAME_D1)
        
        try:
            # Set timezone to UTC
            timezone = pytz.timezone("Etc/UTC")
            
            # Set start date to one year ago from today
            start_date = datetime.now(timezone) - timedelta(days=365)
            
            # Retrieve a large number of bars (max historical data)
            # Use 2000 as a reasonable limit, adjust if needed
            rates = mt5.copy_rates_from(symbol, mt5_timeframe, start_date, 2000000)
            
            if rates is None or len(rates) == 0:
                logger.warning(f"No data found for {symbol} at {timeframe} timeframe")
                return None

            # Convert to DataFrame
            df = pd.DataFrame(rates)
            
            # Convert timestamp 
            df['timestamp'] = pd.to_datetime(df['time'], unit='s', utc=True)
            
            # Add additional metadata
            df['symbol'] = symbol
            df['timeframe'] = timeframe
            
            logger.info(f"Data points for {symbol} ({timeframe}): {len(df)}")
            logger.info(f"Data range for {symbol} ({timeframe}): "
                        f"{df['timestamp'].min()} to {df['timestamp'].max()}")
            
            return df

        except Exception as e:
            logger.error(f"Error fetching data for {symbol} at {timeframe}: {e}")
            return None

    def store_to_mongodb(self, dataframe):
        """
        Store DataFrame to MongoDB with improved collection naming and upsert logic
        """
        if dataframe is None or dataframe.empty:
            return

        # Create collection name based on symbol and timeframe
        collection_name = f"{dataframe['symbol'].iloc[0]}_{dataframe['timeframe'].iloc[0]}"
        collection = self.db[collection_name]

        # Create indexes for efficient querying
        collection.create_index([('timestamp', 1), ('symbol', 1), ('timeframe', 1)], unique=True)

        # Convert to records and upsert based on timestamp
        records = dataframe.to_dict('records')
        
        bulk_operations = []
        for record in records:
            bulk_operations.append(
                UpdateOne(
                    {
                        'timestamp': record['timestamp'],
                        'symbol': record['symbol'],
                        'timeframe': record['timeframe']
                    },
                    {'$set': record},
                    upsert=True
                )
            )

        # Perform bulk write to improve performance
        if bulk_operations:
            result = collection.bulk_write(bulk_operations)
            logger.info(f"Upserted {result.upserted_count} records, Modified {result.modified_count} records")

    def extract_and_store(self):
        """
        Main extraction and storage process
        """
        if not self.connect_mt5():
            return

        for symbol in self.symbols:
            for timeframe in self.selected_timeframes:
                logger.info(f"Processing symbol: {symbol}, Timeframe: {timeframe}")
                try:
                    historical_data = self.fetch_historical_data(symbol, timeframe)
                    if historical_data is not None:
                        self.store_to_mongodb(historical_data)
                        logger.info(f"Successfully processed {symbol} at {timeframe}")
                except Exception as e:
                    logger.error(f"Error processing {symbol} at {timeframe}: {e}")

        # Shutdown MT5 connection
        mt5.shutdown()

def main():
    extractor = MT5DataExtractor()
    extractor.extract_and_store()

if __name__ == "__main__":
    main()