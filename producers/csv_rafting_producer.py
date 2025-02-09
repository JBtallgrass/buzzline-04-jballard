"""
csv_rafting_producer.py

Consumes structured CSV-style messages from Kafka (`rafting_csv_feedback`)
and republishes processed messages to another Kafka topic (`processed_csv_feedback`).

This script:
- Reads CSV-formatted rafting feedback from Kafka.
- Assigns trip status based on feedback type.
- Flags possible trip disruptions due to bad weather.
- Publishes structured messages back to Kafka.
"""

#####################################
# Import Modules
#####################################

import json
import time
from kafka import KafkaConsumer, KafkaProducer
from utils.utils_logger import logger

#####################################
# Kafka Configuration
#####################################

KAFKA_SOURCE_TOPIC = "rafting_csv_feedback"  # Topic with structured CSV data
KAFKA_TARGET_TOPIC = "processed_csv_feedback"  # Topic for processed messages
KAFKA_BROKER = "localhost:9092"

# Create Kafka Consumer (Reads CSV-Formatted Messages)
consumer = KafkaConsumer(
    KAFKA_SOURCE_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    group_id="csv_producer_group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# Create Kafka Producer (Publishes Processed Messages)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

#####################################
# Function to Process CSV Data
#####################################

def process_csv_data(csv_data):
    """
    Process incoming CSV-formatted data before publishing.
    
    Enhancements:
    - Assigns trip status based on feedback.
    - Adds weather and river summary fields.
    - Flags potential trip disruptions due to bad weather.
    """
    guide = csv_data.get("guide", "Unknown")
    is_negative = csv_data.get("is_negative", "no") == "yes"
    weather = csv_data.get("weather", "Unknown")

    # Assign trip status based on feedback
    csv_data["status"] = "negative_feedback" if is_negative else "positive_feedback"

    # Identify potential trip disruptions
    if weather in ["Stormy", "Heavy Rain", "Extreme Winds"]:
        csv_data["trip_disruption"] = "possible"
        logger.warning(f"⚠️ Trip may be disrupted due to bad weather: {weather}")

    # Log processed data
    logger.info(f"✅ Processed CSV Data: {csv_data}")

    return csv_data

#####################################
# Consume, Process, and Publish Messages
#####################################

for message in consumer:
    csv_data = message.value

    # Process the message
    processed_data = process_csv_data(csv_data)

    # Publish to the next Kafka topic
    producer.send(KAFKA_TARGET_TOPIC, value=processed_data)
    logger.info(f"🚀 Republished Processed CSV Data to {KAFKA_TARGET_TOPIC}")

    time.sleep(1)  # Simulating real-time processing
