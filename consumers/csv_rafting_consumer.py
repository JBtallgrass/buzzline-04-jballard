"""
rafting_consumer.py

Consumes JSON messages from a Kafka topic (`rafting_feedback`) 
and republishes processed messages to another Kafka topic (`rafting_csv_feedback`).

This script:
- Logs ALL customer feedback.
- Flags negative comments with a red 🛑.
- Logs environmental data (weather & river conditions).
- Tracks weekly guide performance trends.
- Publishes structured feedback messages to a CSV-friendly Kafka topic.
"""

#####################################
# Import Modules
#####################################

import os
import json
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Kafka Configuration
#####################################

KAFKA_SOURCE_TOPIC = "rafting_feedback"
KAFKA_TARGET_TOPIC = "rafting_csv_feedback"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

# Create Kafka Consumer (Reads JSON feedback)
consumer = KafkaConsumer(
    KAFKA_SOURCE_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    group_id="rafting_csv_transform_group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# Create Kafka Producer (Publishes processed CSV-style messages)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

#####################################
# Load Weather & River Data
#####################################

def load_json_data(file_path: str) -> dict:
    """Load JSON data from a given file path."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return {entry["date"]: entry for entry in json.load(f)}
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return {}
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON format in file: {file_path}")
        return {}

# Load environmental data
WEATHER_DATA_FILE = "data/weather_conditions.json"
RIVER_FLOW_DATA_FILE = "data/river_flow.json"

weather_lookup = load_json_data(WEATHER_DATA_FILE)
river_lookup = load_json_data(RIVER_FLOW_DATA_FILE)

#####################################
# Tracking Data
#####################################

# Track feedback per guide
guide_feedback = defaultdict(lambda: {"positive": 0, "negative": 0})

# Track weekly guide performance
weekly_feedback = defaultdict(lambda: {"positive": 0, "negative": 0})

#####################################
# Function to Process a Message and Publish
#####################################

def process_message(message: dict) -> None:
    """
    Process a JSON message from Kafka and republish it in CSV format.

    Args:
        message (dict): The JSON message.
    """
    try:
        guide = message.get("guide", "unknown")
        comment = message.get("comment", "No comment provided")
        is_negative = message.get("is_negative", False)
        trip_date = message.get("date", "unknown")

        # Get week number for trend analysis
        try:
            week_number = datetime.strptime(trip_date, "%Y-%m-%d").isocalendar()[1]
        except ValueError:
            logger.error(f"Invalid date format in message: {trip_date}")
            return

        # Get environmental conditions for this date
        weather = weather_lookup.get(trip_date, {
            "weather_condition": "Data Not Available",
            "temperature": "N/A",
            "wind_speed": "N/A",
            "precipitation": "N/A"
        })

        river = river_lookup.get(trip_date, {
            "river_flow": "N/A",
            "water_level": "N/A",
            "water_temperature": "N/A"
        })

        weather_summary = (
            f"🌤 {weather.get('weather_condition')} | "
            f"🌡 {weather.get('temperature')}°F | "
            f"💨 Wind {weather.get('wind_speed')} mph | "
            f"🌧 {weather.get('precipitation')} inches rain"
        )

        river_summary = (
            f"🌊 Flow {river.get('river_flow')} cfs | "
            f"📏 Water Level {river.get('water_level')} ft | "
            f"🌡 Water Temp {river.get('water_temperature')}°F"
        )

        # Flag negative comments with a red 🛑
        if is_negative:
            comment = f"🛑 {comment}"
            guide_feedback[guide]["negative"] += 1
            weekly_feedback[(guide, week_number)]["negative"] += 1

        else:
            guide_feedback[guide]["positive"] += 1
            weekly_feedback[(guide, week_number)]["positive"] += 1

        # Log processed feedback
        logger.info(f"📝 Feedback ({trip_date}) | Guide: {guide} | Comment: {comment}")
        logger.info(f"⛅ {weather_summary}")
        logger.info(f"🌊 {river_summary}")

        # Publish structured message to `rafting_csv_feedback`
        csv_data = {
            "timestamp": message.get("timestamp"),
            "date": trip_date,
            "guide": guide,
            "comment": comment,
            "trip_type": message.get("trip_type", "unknown"),
            "is_negative": "yes" if is_negative else "no",
            "weather": weather.get("weather_condition", "Unknown"),
            "temperature": weather.get("temperature", "N/A"),
            "wind_speed": weather.get("wind_speed", "N/A"),
            "rainfall": weather.get("precipitation", "N/A"),
            "river_flow": river.get("river_flow", "N/A"),
            "water_level": river.get("water_level", "N/A"),
            "water_temperature": river.get("water_temperature", "N/A")
        }

        producer.send(KAFKA_TARGET_TOPIC, value=csv_data)
        logger.info(f"✅ Published CSV-formatted data to Kafka: {csv_data}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Define Main Function for Kafka Processing
#####################################

def main() -> None:
    """
    Main entry point for the consumer.

    - Reads JSON messages from `rafting_feedback`.
    - Converts them into a CSV-friendly format.
    - Publishes them to `rafting_csv_feedback`.
    """
    logger.info("🚀 START rafting JSON-to-CSV consumer.")

    # Process messages
    try:
        for message in consumer:
            process_message(message.value)
    except KeyboardInterrupt:
        logger.warning("⚠️ Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"❌ Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info("✅ Kafka consumer closed.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
