import os
import json
import time
import pandas as pd
import matplotlib.pyplot as plt
from kafka import KafkaConsumer
from dotenv import load_dotenv
from pathlib import Path
from loguru import logger

# Configure loguru
LOG_FOLDER = Path("logs")
LOG_FOLDER.mkdir(parents=True, exist_ok=True)  # Ensure logs directory exists
logger.add(LOG_FOLDER / "app.log", rotation="10 MB", retention="7 days", level="INFO", format="{time} | {level} | {message}")

# Load environment variables
load_dotenv()

# Validate essential environment variables
KAFKA_BROKER = os.getenv("KAFKA_BROKER_ADDRESS")
TOPIC = os.getenv("RAFTING_PROCESSED_TOPIC")
if not KAFKA_BROKER or not TOPIC:
    logger.critical("❌ Missing required environment variables: KAFKA_BROKER_ADDRESS or RAFTING_PROCESSED_TOPIC.")
    raise EnvironmentError("Missing required environment variables.")

IMAGES_FOLDER = Path(os.getenv("IMAGES_FOLDER", "images"))
IMAGES_FOLDER.mkdir(parents=True, exist_ok=True)  # Ensure images directory exists

# Initialize Kafka Consumer with error handling
try:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        group_id="real_time_consumer",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    logger.info(f"✅ Connected to Kafka topic: {TOPIC}")
except Exception as e:
    logger.critical(f"❌ Failed to connect to Kafka: {e}")
    raise

# Data storage for visualization
data = []

def save_chart(df, chart_title, file_name, chart_type="bar"):
    """Custom function to create and save a chart."""
    if df.empty:
        logger.warning(f"⚠️ No data available for {chart_title}. Chart skipped.")
        return

    plt.figure(figsize=(10, 6))
    try:
        if chart_type == "bar":
            df.plot(kind="bar", color="blue", legend=False)
        elif chart_type == "line":
            df.plot(kind="line", marker="o", legend=False)
        elif chart_type == "box":
            df.boxplot(column=["river_flow"], by="is_negative")
        plt.title(chart_title)
        plt.tight_layout()
        plt.savefig(IMAGES_FOLDER / file_name)
        logger.info(f"✅ Chart saved: {file_name}")
        plt.close()
    except Exception as e:
        logger.error(f"❌ Error generating chart '{chart_title}': {e}")

# Process messages and update visualizations
try:
    for message in consumer:
        try:
            feedback = message.value
            # Validate required fields
            if not all(key in feedback for key in ["date", "status", "weather", "river_flow", "is_negative"]):
                logger.warning(f"⚠️ Skipping invalid message: {feedback}")
                continue

            data.append(feedback)

            # Convert to DataFrame every 10 messages and update charts
            if len(data) % 10 == 0:
                df = pd.DataFrame(data)

                # Positive vs. Negative Feedback (Bar Chart)
                feedback_counts = df["status"].value_counts()
                save_chart(feedback_counts, "Positive vs Negative Feedback", "feedback_counts.png", "bar")

                # Weekly Feedback Trends (Line Chart)
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
                df["week"] = df["date"].dt.isocalendar().week
                weekly_trends = df.groupby("week").size()
                save_chart(weekly_trends, "Weekly Feedback Trends", "weekly_trends.png", "line")

                # Weather Conditions vs. Negative Feedback (Bar Chart)
                weather_impact = df[df["is_negative"] == 1].groupby("weather").size()
                save_chart(weather_impact, "Weather vs Negative Feedback", "weather_vs_feedback.png", "bar")

                # River Flow vs. Feedback Type (Box Plot)
                save_chart(df, "River Flow vs Feedback Type", "river_flow_vs_feedback.png", "box")

                logger.info(f"✅ Charts updated and saved in the 'images/' directory.")
        except json.JSONDecodeError:
            logger.error("❌ Failed to decode JSON message.")
        except Exception as e:
            logger.error(f"❌ Error processing message: {e}", exc_info=True)

        time.sleep(1)  # Simulate real-time delay

except KeyboardInterrupt:
    logger.warning("⛔ Consumer interrupted by user.")
finally:
    consumer.close()
    logger.info("✅ Kafka consumer closed.")
