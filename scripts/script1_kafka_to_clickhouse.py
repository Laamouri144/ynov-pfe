"""
Script 1: Kafka to ClickHouse Consumer
Continuously reads airline delay data from Kafka and inserts into ClickHouse
Run on PC1
"""

import os
import json
import time
from datetime import datetime
from typing import Dict, List
from confluent_kafka import Consumer, KafkaError
import clickhouse_connect
from loguru import logger
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
KAFKA_HOST = os.getenv('KAFKA_HOST', 'localhost')
KAFKA_PORT = os.getenv('KAFKA_PORT', '9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'airline-delays')
CONSUMER_GROUP_ID = os.getenv('CONSUMER_GROUP_ID', 'airline-consumer-group')

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_HTTP_PORT', '8123'))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE', 'airline_data')

BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))

# Setup logging
logger.add("logs/kafka_to_clickhouse.log", rotation="100 MB", retention="10 days")


class KafkaToClickHouseConsumer:
    """Consumes messages from Kafka and writes to ClickHouse"""

    def __init__(self):
        self.consumer = None
        self.clickhouse_client = None
        self.batch = []
        self.total_processed = 0
        self.initialize_connections()

    def initialize_connections(self):
        """Initialize Kafka consumer and ClickHouse client"""
        # Kafka Consumer
        kafka_config = {
            'bootstrap.servers': f'{KAFKA_HOST}:{KAFKA_PORT}',
            'group.id': CONSUMER_GROUP_ID,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000,
        }
        
        self.consumer = Consumer(kafka_config)
        self.consumer.subscribe([KAFKA_TOPIC])
        logger.info(f"Kafka consumer initialized: {KAFKA_HOST}:{KAFKA_PORT}, topic: {KAFKA_TOPIC}")

        # ClickHouse Client
        self.clickhouse_client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE
        )
        logger.info(f"ClickHouse client initialized: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")

    def process_message(self, message_value: str) -> Dict:
        """Parse and transform Kafka message"""
        try:
            data = json.loads(message_value)
            
            # Transform data for ClickHouse
            return {
                'id': hash(f"{data['year']}{data['month']}{data['carrier']}{data['airport']}"),
                'year': int(data.get('year', 0)),
                'month': int(data.get('month', 0)),
                'carrier': data.get('carrier', ''),
                'carrier_name': data.get('carrier_name', ''),
                'airport': data.get('airport', ''),
                'airport_name': data.get('airport_name', ''),
                'arr_flights': int(data.get('arr_flights', 0)),
                'arr_del15': int(data.get('arr_del15', 0)),
                'carrier_ct': float(data.get('carrier_ct', 0.0)),
                'weather_ct': float(data.get('weather_ct', 0.0)),
                'nas_ct': float(data.get('nas_ct', 0.0)),
                'security_ct': float(data.get('security_ct', 0.0)),
                'late_aircraft_ct': float(data.get('late_aircraft_ct', 0.0)),
                'arr_cancelled': int(data.get('arr_cancelled', 0)),
                'arr_diverted': int(data.get('arr_diverted', 0)),
                'arr_delay': int(data.get('arr_delay', 0)),
                'carrier_delay': int(data.get('carrier_delay', 0)),
                'weather_delay': int(data.get('weather_delay', 0)),
                'nas_delay': int(data.get('nas_delay', 0)),
                'security_delay': int(data.get('security_delay', 0)),
                'late_aircraft_delay': int(data.get('late_aircraft_delay', 0)),
            }
        except Exception as e:
            logger.error(f"Error processing message: {e}, message: {message_value}")
            return None

    def insert_batch(self):
        """Insert batch of records into ClickHouse"""
        if not self.batch:
            return

        try:
            # Prepare data for insertion
            columns = list(self.batch[0].keys())
            data = [[record[col] for col in columns] for record in self.batch]

            # Insert into ClickHouse
            self.clickhouse_client.insert(
                'flights',
                data,
                column_names=columns
            )

            logger.info(f"Inserted {len(self.batch)} records into ClickHouse")
            self.total_processed += len(self.batch)
            self.batch = []

        except Exception as e:
            logger.error(f"Error inserting batch: {e}")
            raise

    def run(self):
        """Main consumer loop"""
        logger.info("Starting Kafka to ClickHouse consumer...")
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                    continue

                # Process message
                message_value = msg.value().decode('utf-8')
                processed_data = self.process_message(message_value)
                
                if processed_data:
                    self.batch.append(processed_data)

                # Insert batch when size reached
                if len(self.batch) >= BATCH_SIZE:
                    self.insert_batch()
                    self.consumer.commit()
                    logger.info(f"Total records processed: {self.total_processed}")

        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            # Insert remaining records
            if self.batch:
                self.insert_batch()
                self.consumer.commit()
            
            self.consumer.close()
            logger.info(f"Consumer stopped. Total records processed: {self.total_processed}")


if __name__ == "__main__":
    consumer = KafkaToClickHouseConsumer()
    consumer.run()
