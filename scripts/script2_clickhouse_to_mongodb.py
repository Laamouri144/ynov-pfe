"""
Script 2: ClickHouse to MongoDB Cache
Periodically extracts aggregated data from ClickHouse and caches in MongoDB
Run on PC2
"""

import os
import time
from datetime import datetime, timedelta
from typing import Dict, List
import clickhouse_connect
from pymongo import MongoClient, UpdateOne
from loguru import logger
from dotenv import load_dotenv
import schedule

# Load environment variables
load_dotenv()

# Configuration
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_HTTP_PORT', '8123'))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE', 'airline_data')

MONGODB_HOST = os.getenv('MONGODB_HOST', 'localhost')
MONGODB_PORT = int(os.getenv('MONGODB_PORT', '27017'))
MONGODB_DATABASE = os.getenv('MONGODB_DATABASE', 'airline_cache')

CACHE_UPDATE_INTERVAL = int(os.getenv('CACHE_UPDATE_INTERVAL', '300'))  # seconds

# Setup logging
logger.add("logs/clickhouse_to_mongodb.log", rotation="100 MB", retention="10 days")


class ClickHouseToMongoDBCache:
    """Extracts data from ClickHouse and caches in MongoDB"""

    def __init__(self):
        self.clickhouse_client = None
        self.mongo_client = None
        self.mongo_db = None
        self.initialize_connections()

    def initialize_connections(self):
        """Initialize ClickHouse and MongoDB connections"""
        # ClickHouse
        self.clickhouse_client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE
        )
        logger.info(f"ClickHouse client initialized: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")

        # MongoDB
        self.mongo_client = MongoClient(f'mongodb://{MONGODB_HOST}:{MONGODB_PORT}/')
        self.mongo_db = self.mongo_client[MONGODB_DATABASE]
        logger.info(f"MongoDB client initialized: {MONGODB_HOST}:{MONGODB_PORT}")

    def cache_daily_aggregates(self):
        """Cache daily aggregated delay statistics"""
        logger.info("Caching daily aggregates...")
        
        query = """
        SELECT
            year,
            month,
            carrier,
            carrier_name,
            airport,
            airport_name,
            sum(arr_flights) as total_flights,
            sum(arr_del15) as total_delayed,
            sum(arr_cancelled) as total_cancelled,
            sum(arr_diverted) as total_diverted,
            sum(arr_delay) as total_delay_minutes,
            sum(carrier_delay) as carrier_delay_minutes,
            sum(weather_delay) as weather_delay_minutes,
            sum(nas_delay) as nas_delay_minutes,
            sum(security_delay) as security_delay_minutes,
            sum(late_aircraft_delay) as late_aircraft_delay_minutes,
            round(sum(arr_del15) * 100.0 / sum(arr_flights), 2) as delay_percentage,
            round(sum(arr_delay) / sum(arr_flights), 2) as avg_delay_per_flight
        FROM flights
        GROUP BY year, month, carrier, carrier_name, airport, airport_name
        """
        
        result = self.clickhouse_client.query(query)
        data = result.result_rows
        columns = result.column_names
        
        # Prepare documents for MongoDB
        documents = []
        for row in data:
            doc = dict(zip(columns, row))
            doc['_id'] = f"{doc['year']}-{doc['month']}-{doc['carrier']}-{doc['airport']}"
            doc['timestamp'] = datetime.now()
            documents.append(doc)
        
        # Bulk upsert into MongoDB
        if documents:
            collection = self.mongo_db['aggregated_delays']
            operations = [
                UpdateOne({'_id': doc['_id']}, {'$set': doc}, upsert=True)
                for doc in documents
            ]
            result = collection.bulk_write(operations)
            logger.info(f"Daily aggregates cached: {result.upserted_count} inserted, {result.modified_count} updated")

    def cache_carrier_stats(self):
        """Cache carrier performance statistics"""
        logger.info("Caching carrier statistics...")
        
        query = """
        SELECT
            carrier,
            carrier_name,
            sum(arr_flights) as total_flights,
            sum(arr_del15) as total_delayed,
            round(sum(arr_del15) * 100.0 / sum(arr_flights), 2) as delay_percentage,
            round(sum(arr_delay) / sum(arr_flights), 2) as avg_delay_minutes,
            sum(carrier_delay) as total_carrier_delay,
            sum(weather_delay) as total_weather_delay,
            sum(nas_delay) as total_nas_delay,
            sum(security_delay) as total_security_delay,
            sum(late_aircraft_delay) as total_late_aircraft_delay
        FROM flights
        GROUP BY carrier, carrier_name
        ORDER BY delay_percentage DESC
        """
        
        result = self.clickhouse_client.query(query)
        data = result.result_rows
        columns = result.column_names
        
        documents = []
        for row in data:
            doc = dict(zip(columns, row))
            doc['_id'] = doc['carrier']
            doc['timestamp'] = datetime.now()
            documents.append(doc)
        
        if documents:
            collection = self.mongo_db['carrier_stats']
            operations = [
                UpdateOne({'_id': doc['_id']}, {'$set': doc}, upsert=True)
                for doc in documents
            ]
            result = collection.bulk_write(operations)
            logger.info(f"Carrier stats cached: {result.upserted_count} inserted, {result.modified_count} updated")

    def cache_airport_stats(self):
        """Cache airport performance statistics"""
        logger.info("Caching airport statistics...")
        
        query = """
        SELECT
            airport,
            airport_name,
            sum(arr_flights) as total_flights,
            sum(arr_del15) as total_delayed,
            round(sum(arr_del15) * 100.0 / sum(arr_flights), 2) as delay_percentage,
            round(sum(arr_delay) / sum(arr_flights), 2) as avg_delay_minutes,
            sum(carrier_delay) as total_carrier_delay,
            sum(weather_delay) as total_weather_delay,
            sum(nas_delay) as total_nas_delay,
            sum(security_delay) as total_security_delay,
            sum(late_aircraft_delay) as total_late_aircraft_delay
        FROM flights
        GROUP BY airport, airport_name
        ORDER BY delay_percentage DESC
        """
        
        result = self.clickhouse_client.query(query)
        data = result.result_rows
        columns = result.column_names
        
        documents = []
        for row in data:
            doc = dict(zip(columns, row))
            doc['_id'] = doc['airport']
            doc['timestamp'] = datetime.now()
            documents.append(doc)
        
        if documents:
            collection = self.mongo_db['airport_stats']
            operations = [
                UpdateOne({'_id': doc['_id']}, {'$set': doc}, upsert=True)
                for doc in documents
            ]
            result = collection.bulk_write(operations)
            logger.info(f"Airport stats cached: {result.upserted_count} inserted, {result.modified_count} updated")

    def cache_time_series_data(self):
        """Cache time series data for trend analysis"""
        logger.info("Caching time series data...")
        
        query = """
        SELECT
            year,
            month,
            sum(arr_flights) as total_flights,
            sum(arr_del15) as total_delayed,
            round(sum(arr_del15) * 100.0 / sum(arr_flights), 2) as delay_percentage,
            sum(arr_delay) as total_delay_minutes,
            sum(arr_cancelled) as total_cancelled,
            sum(arr_diverted) as total_diverted
        FROM flights
        GROUP BY year, month
        ORDER BY year, month
        """
        
        result = self.clickhouse_client.query(query)
        data = result.result_rows
        columns = result.column_names
        
        documents = []
        for row in data:
            doc = dict(zip(columns, row))
            doc['_id'] = f"{doc['year']}-{doc['month']:02d}"
            doc['timestamp'] = datetime.now()
            documents.append(doc)
        
        if documents:
            collection = self.mongo_db['time_series']
            operations = [
                UpdateOne({'_id': doc['_id']}, {'$set': doc}, upsert=True)
                for doc in documents
            ]
            result = collection.bulk_write(operations)
            logger.info(f"Time series data cached: {result.upserted_count} inserted, {result.modified_count} updated")

    def update_cache(self):
        """Update all cache collections"""
        try:
            start_time = time.time()
            logger.info("=" * 50)
            logger.info("Starting cache update...")
            
            self.cache_daily_aggregates()
            self.cache_carrier_stats()
            self.cache_airport_stats()
            self.cache_time_series_data()
            
            duration = time.time() - start_time
            logger.info(f"Cache update completed in {duration:.2f} seconds")
            logger.info("=" * 50)
            
        except Exception as e:
            logger.error(f"Error updating cache: {e}")

    def run_scheduler(self):
        """Run periodic cache updates"""
        logger.info(f"Starting cache scheduler (interval: {CACHE_UPDATE_INTERVAL} seconds)...")
        
        # Initial update
        self.update_cache()
        
        # Schedule periodic updates
        schedule.every(CACHE_UPDATE_INTERVAL).seconds.do(self.update_cache)
        
        while True:
            schedule.run_pending()
            time.sleep(1)

    def run_once(self):
        """Run cache update once and exit"""
        logger.info("Running cache update (one-time)...")
        self.update_cache()


if __name__ == "__main__":
    import sys
    
    cache_manager = ClickHouseToMongoDBCache()
    
    # Check if running once or continuous
    if len(sys.argv) > 1 and sys.argv[1] == '--once':
        cache_manager.run_once()
    else:
        cache_manager.run_scheduler()
