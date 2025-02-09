import json
import time
import matplotlib.pyplot as plt
from kafka import KafkaConsumer
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
KAFKA_BROKER = os.getenv("KAFKA_BROKER_ADDRESS", "localhost:9092")
TOPIC = os.getenv("RAFTING_PROCESSED_TOPIC", "processed_csv_feedback")

# Create a Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    group_id="my_consumer_group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# Data storage for visualization
feedback_counts = {"positive_feedback": 0, "negative_feedback": 0}

def update_chart():
    """Updates the real-time bar chart."""
    plt.clf()  # Clear the current figure
    plt.bar(feedback_counts.keys(), feedback_counts.values(), color=["green", "red"])
    plt.xlabel("Feedback Type")
    plt.ylabel("Count")
    plt.title("Real-Time Feedback Counts")
    plt.pause(0.1)  # Pause for a brief moment to update the chart

# Real-time data processing and visualization
plt.ion()  # Turn on interactive mode for real-time updates
plt.figure(figsize=(8, 6))

for message in consumer:
    feedback = message.value
    status = feedback.get("status", "unknown")
    
    if status in feedback_counts:
        feedback_counts[status] += 1

    print(f"Processed Feedback: {feedback}")
    update_chart()
    time.sleep(0.5)  # Simulate real-time delay

plt.ioff()  # Turn off interactive mode
plt.show()
