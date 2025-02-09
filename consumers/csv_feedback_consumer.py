import os
import json
import csv
from kafka import KafkaConsumer
from dotenv import load_dotenv
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "rafting_csv_feedback"
CSV_FILE = "data/rafting_feedback.csv"

#####################################
# Create Kafka Consumer
#####################################

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    group_id="rafting_csv_analysis_group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

#####################################
# Function to Save Messages to CSV
#####################################

def save_to_csv(message):
    """
    Append processed feedback messages to a CSV file.
    """
    file_exists = os.path.isfile(CSV_FILE)
    
    with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        # Write header if file does not exist
        if not file_exists:
            writer.writerow([
                "timestamp", "date", "guide", "comment", "trip_type", "is_negative",
                "weather", "temperature", "wind_speed", "rainfall",
                "river_flow", "water_level", "water_temperature"
            ])

        # Write the new row
        writer.writerow([
            message["timestamp"],
            message["date"],
            message["guide"],
            message["comment"],
            message["trip_type"],
            message["is_negative"],
            message["weather"],
            message["temperature"],
            message["wind_speed"],
            message["rainfall"],
            message["river_flow"],
            message["water_level"],
            message["water_temperature"]
        ])
    
    logger.info(f"✅ Saved message to CSV: {message}")

#####################################
# Main Function
#####################################

def main():
    logger.info("🚀 START CSV consumer and writer.")
    
    try:
        for message in consumer:
            save_to_csv(message.value)
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
