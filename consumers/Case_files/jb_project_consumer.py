import os
import json
import time
from collections import defaultdict
from datetime import datetime
from kafka import KafkaConsumer
from dotenv import load_dotenv
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from utils.utils_logger import logger  # Ensure this exists in your utils module

#####################################
# Load Environment Variables
#####################################

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("RAFTING_TOPIC", "rafting_feedback")

if not KAFKA_BROKER or not KAFKA_TOPIC:
    logger.critical("❌ Missing required environment variables.")
    raise EnvironmentError("Missing required environment variables.")

#####################################
# Initialize Tracking Data
#####################################

data_buffer = []
guide_feedback = defaultdict(lambda: {"positive": 0, "negative": 0})
weekly_feedback = defaultdict(lambda: {"positive": 0, "negative": 0})

#####################################
# Message Processing
#####################################

def process_message(message: dict):
    """Process a single Kafka message and update tracking data."""
    try:
        guide = message.get("guide", "unknown")
        is_negative = message.get("is_negative", False)
        trip_date = message.get("date", "unknown")

        # Get week number for trend analysis
        week_number = datetime.strptime(trip_date, "%Y-%m-%d").isocalendar()[1]

        # Update feedback tracking
        feedback_type = "negative" if is_negative else "positive"
        guide_feedback[guide][feedback_type] += 1
        weekly_feedback[(guide, week_number)][feedback_type] += 1

    except Exception as e:
        logger.error(f"❌ Error processing message: {e}")

#####################################
# Real-Time Visualization
#####################################

def update_chart(frame):
    """Update the chart with new data."""
    if not data_buffer:
        return

    df = pd.DataFrame(data_buffer)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["week"] = df["date"].dt.isocalendar().week

    # Plot positive vs. negative feedback
    plt.cla()
    feedback_counts = df["is_negative"].value_counts()
    feedback_counts.index = ["Positive", "Negative"]
    feedback_counts.plot(kind="bar", color=["green", "red"], ax=plt.gca())
    plt.title("Positive vs Negative Feedback (Real-Time)")
    plt.xlabel("Feedback Type")
    plt.ylabel("Count")
    plt.tight_layout()

#####################################
# Main Kafka Consumer Loop
#####################################

def main():
    """Main entry point for the Kafka consumer with live visualization."""
    logger.info("🚀 Starting Kafka consumer for rafting feedback.")
    
    # Create Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        group_id="rafting_consumer_group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    fig = plt.figure(figsize=(10, 6))
    ani = FuncAnimation(fig, update_chart, interval=2000)  # Update every 2 seconds

    try:
        for message in consumer:
            message_dict = message.value
            process_message(message_dict)
            data_buffer.append(message_dict)

            # Limit buffer size to prevent memory overflow
            if len(data_buffer) > 1000:
                data_buffer.pop(0)

            time.sleep(0.5)  # Simulate real-time delay

    except KeyboardInterrupt:
        logger.warning("⚠️ Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"❌ Error in Kafka consumer: {e}")
    finally:
        consumer.close()
        logger.info("✅ Kafka consumer closed.")
    
    plt.show()

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
