"""
NiFi Simulator - Alternative to NiFi for Testing
Simulates NiFi by reading CSV and publishing to Kafka
Use this if NiFi is too resource-intensive
"""

import os
import csv
import json
import time
from datetime import datetime
from confluent_kafka import Producer
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

# Configuration
KAFKA_HOST = os.getenv('KAFKA_HOST', 'localhost')
KAFKA_PORT = os.getenv('KAFKA_PORT', '9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'airline-delays')
CSV_FILE = os.getenv('CSV_FILE_PATH', './Airline_Delay_Cause - Airline_Delay_Cause.csv')

# Simulation settings
RECORDS_PER_SECOND = 10  # Adjust for speed
LOOP_DATASET = True  # Repeat dataset when finished

logger.add("logs/nifi_simulator.log", rotation="100 MB")


class NiFiSimulator:
    """Simulates NiFi data flow"""

    def __init__(self):
        self.producer = None
        self.total_sent = 0
        self.initialize_kafka()

    def initialize_kafka(self):
        """Initialize Kafka producer"""
        kafka_config = {
            'bootstrap.servers': f'{KAFKA_HOST}:{KAFKA_PORT}',
            'client.id': 'nifi-simulator'
        }
        self.producer = Producer(kafka_config)
        logger.info(f"Kafka producer initialized: {KAFKA_HOST}:{KAFKA_PORT}")

    def delivery_callback(self, err, msg):
        """Callback for message delivery"""
        if err:
            logger.error(f'Message delivery failed: {err}')
        else:
            self.total_sent += 1
            if self.total_sent % 100 == 0:
                logger.info(f'Messages sent: {self.total_sent}')

    def read_and_stream_csv(self):
        """Read CSV and stream to Kafka"""
        logger.info(f"Starting CSV streaming from: {CSV_FILE}")
        
        if not os.path.exists(CSV_FILE):
            logger.error(f"CSV file not found: {CSV_FILE}")
            return

        while True:
            with open(CSV_FILE, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                
                for row in reader:
                    # Add timestamp
                    row['ingestion_timestamp'] = datetime.now().isoformat()
                    
                    # Convert to JSON
                    message = json.dumps(row)
                    
                    # Send to Kafka
                    self.producer.produce(
                        KAFKA_TOPIC,
                        value=message.encode('utf-8'),
                        callback=self.delivery_callback
                    )
                    
                    # Poll for delivery reports
                    self.producer.poll(0)
                    
                    # Rate limiting
                    time.sleep(1.0 / RECORDS_PER_SECOND)
                
                # Flush remaining messages
                self.producer.flush()
                
                logger.info(f"Finished streaming CSV. Total sent: {self.total_sent}")
                
                if not LOOP_DATASET:
                    break
                
                logger.info("Looping dataset...")

    def run(self):
        """Main execution"""
        try:
            self.read_and_stream_csv()
        except KeyboardInterrupt:
            logger.info("Shutting down simulator...")
        except Exception as e:
            logger.error(f"Error: {e}")
        finally:
            self.producer.flush()
            logger.info(f"Total messages sent: {self.total_sent}")


if __name__ == "__main__":
    simulator = NiFiSimulator()
    simulator.run()
