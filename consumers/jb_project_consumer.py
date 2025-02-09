#####################################
# jb_project_consumer.py
#
# Consumes JSON messages from Kafka topic `rafting_feedback`
# Processes rafting trip feedback, tracks weekly trends, and
# saves feedback and performance data.
#####################################

import os
import json
import time
from collections import defaultdict
from datetime import datetime
from kafka import KafkaConsumer
from dotenv import load_dotenv
from utils.utils_logger import logger  # Ensure this exists in your utils module
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

#####################################
# Load Environment Variables
#####################################

load_dotenv()

# Kafka Config
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("RAFTING_TOPIC", "rafting_feedback")

# File Paths
DATA_FOLDER = Path("data")
IMAGES_FOLDER = Path(os.getenv("IMAGES_FOLDER", "images"))
IMAGES_FOLDER.mkdir(parents=True, exist_ok=True)

# Validate Essential Environment Variables
if not KAFKA_BROKER or not KAFKA_TOPIC:
    logger.critical("❌ Missing required environment variables.")
    raise EnvironmentError("Missing required environment variables.")

#####################################
# Load Environmental Data
#####################################

def load_json_data(file_path: Path) -> dict:
    """Load JSON data from a file."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return {entry["date"]: entry for entry in json.load(f)}
    except FileNotFoundError:
        logger.error(f"❌ File not found: {file_path}")
        return {}
    except json.JSONDecodeError:
        logger.error(f"❌ Invalid JSON format in file: {file_path}")
        return {}

# Load weather and river data
weather_lookup = load_json_data(DATA_FOLDER / "weather_conditions.json")
river_lookup = load_json_data(DATA_FOLDER / "river_flow.json")

#####################################
# Initialize Tracking Data
#####################################

guide_feedback = defaultdict(lambda: {"positive": 0, "negative": 0})
weekly_feedback = defaultdict(lambda: {"positive": 0, "negative": 0})
data_buffer = []

#####################################
# Chart Generation Helper
#####################################

def save_chart(df, chart_title, file_name, chart_type="bar"):
    """Generate and save a chart to the images directory."""
    if df.empty:
        logger.warning(f"⚠️ No data for {chart_title}. Chart skipped.")
        return

    plt.figure(figsize=(10, 6))
    try:
        if chart_type == "bar":
            df.plot(kind="bar", legend=False)
        elif chart_type == "line":
            df.plot(kind="line", marker="o", legend=False)
        elif chart_type == "box":
            df.boxplot(column=["river_flow"], by="is_negative")

        plt.title(chart_title)
        plt.tight_layout()
        plt.savefig(IMAGES_FOLDER / file_name)
        plt.close()
        logger.info(f"✅ Chart saved: {file_name}")
    except Exception as e:
        logger.error(f"❌ Error generating chart '{chart_title}': {e}")

#####################################
# Message Processing
#####################################

def process_message(message: dict) -> None:
    """Process a single message and update tracking data."""
    try:
        guide = message.get("guide", "unknown")
        comment = message.get("comment", "No comment provided")
        is_negative = message.get("is_negative", False)
        trip_date = message.get("date", "unknown")

        # Get week number for trend analysis
        week_number = datetime.strptime(trip_date, "%Y-%m-%d").isocalendar()[1]

        # Get environmental conditions
        weather = weather_lookup.get(trip_date, {"weather_condition": "Unknown"})
        river = river_lookup.get(trip_date, {"river_flow": "N/A", "water_level": "N/A"})

        # Update feedback tracking
        feedback_type = "negative" if is_negative else "positive"
        guide_feedback[guide][feedback_type] += 1
        weekly_feedback[(guide, week_number)][feedback_type] += 1

        # Log feedback details
        logger.info(f"📝 Feedback ({trip_date}) | Guide: {guide} | Comment: {comment}")
        logger.info(f"⛅ Weather: {weather.get('weather_condition')}, River Flow: {river.get('river_flow')} cfs")

    except ValueError:
        logger.error(f"❌ Invalid date format in message: {message.get('date')}")
    except Exception as e:
        logger.error(f"❌ Error processing message: {e}")

#####################################
# Main Kafka Consumer Loop
#####################################

def main():
    """Main entry point for the Kafka consumer."""
    logger.info("🚀 Starting Kafka consumer for rafting feedback.")

    # Create Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        group_id="rafting_consumer_group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    try:
        for message in consumer:
            try:
                process_message(message.value)
                data_buffer.append(message.value)

                # Process and save charts every 10 messages
                if len(data_buffer) % 10 == 0:
                    df = pd.DataFrame(data_buffer)
                    df["date"] = pd.to_datetime(df["date"], errors="coerce")
                    df["week"] = df["date"].dt.isocalendar().week

                    # Generate charts
                    feedback_counts = df["is_negative"].value_counts()
                    save_chart(feedback_counts, "Positive vs Negative Feedback", "feedback_counts.png")

                    weekly_trends = df.groupby("week").size()
                    save_chart(weekly_trends, "Weekly Feedback Trends", "weekly_trends.png", "line")

                    logger.info("✅ Charts updated.")
                    data_buffer.clear()  # Clear buffer after processing

            except json.JSONDecodeError:
                logger.error("❌ Failed to decode JSON message.")
            except Exception as e:
                logger.error(f"❌ Error processing individual message: {e}")

            time.sleep(1)  # Simulate real-time processing delay

    except KeyboardInterrupt:
        logger.warning("⚠️ Consumer interrupted by user.")
    finally:
        consumer.close()
        logger.info("✅ Kafka consumer closed.")

#####################################
# Execute Main
#####################################

if __name__ == "__main__":
    main()
