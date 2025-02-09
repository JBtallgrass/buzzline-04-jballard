import os
import json
import time
import pandas as pd
import matplotlib.pyplot as plt
from kafka import KafkaConsumer
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv()
KAFKA_BROKER = os.getenv("KAFKA_BROKER_ADDRESS", "localhost:9092")
TOPIC = os.getenv("RAFTING_PROCESSED_TOPIC", "processed_csv_feedback")
IMAGES_FOLDER = Path("images")
IMAGES_FOLDER.mkdir(parents=True, exist_ok=True)  # Ensure images directory exists

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    group_id="real_time_consumer",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# Data storage for visualization
data = []

def save_chart(df, chart_title, file_name, chart_type="bar"):
    """Custom function to create and save a chart."""
    plt.figure(figsize=(10, 6))
    if chart_type == "bar":
        df.plot(kind="bar", color="blue")
    elif chart_type == "line":
        df.plot(kind="line", marker="o")
    elif chart_type == "box":
        df.boxplot(column=["river_flow"], by="is_negative")
    plt.title(chart_title)
    plt.tight_layout()
    plt.savefig(IMAGES_FOLDER / file_name)
    plt.close()

# Process messages and update visualizations
for message in consumer:
    feedback = message.value
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

        print(f"✅ Charts updated and saved in the 'images/' directory.")
    time.sleep(1)  # Simulate real-time delay
