import os
import sys
import time
import pathlib
import json
import subprocess
from dotenv import load_dotenv

# Import Kafka utilities
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

#####################################
# Function to Run Data Generators
#####################################

def run_data_generators():
    """
    Run all data generation scripts from the `utils/` folder before starting Kafka producer.
    """
    scripts = [
        "utils_generate_rafting_data.py",
        "utils_generate_river_flow.py",
        "utils_generate_weather_data.py"
    ]

    # Define the correct directory where the scripts are located
    utils_folder = pathlib.Path(__file__).parent.parent.joinpath("utils")  # Move up one level, then into 'utils'

    for script in scripts:
        script_path = utils_folder.joinpath(script)  # Adjusted path

        if script_path.exists():
            logger.info(f"Running data generator: {script_path}")
            try:
                subprocess.run(["python", str(script_path)], check=True)
                logger.info(f"✅ Data generation successful: {script}")
            except subprocess.CalledProcessError as e:
                logger.error(f"❌ Failed to generate data from {script}: {e}")
                sys.exit(1)  # Exit if a script fails
        else:
            logger.error(f"❌ Script not found: {script_path}. Exiting.")
            sys.exit(1)  # Exit if the script is missing

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Set up Paths
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent  # Go one level up
DATA_FOLDER: pathlib.Path = PROJECT_ROOT.joinpath("data")
DATA_FILE: pathlib.Path = DATA_FOLDER.joinpath("all_rafting_remarks.json")

#####################################
# Main Function
#####################################

def main():
    """
    Main entry point for the producer:
    - Runs all data generation scripts.
    - Ensures Kafka topic exists.
    - Creates Kafka producer.
    - Streams messages from JSON file to Kafka.
    """

    logger.info("🚀 START: Rafting Producer")

    # Step 1: Run all data generators **before** Kafka starts
    run_data_generators()

    # Step 2: Verify Kafka Services
    verify_services()

    # Step 3: Get Kafka topic and message interval
    topic = os.getenv("RAFTING_TOPIC", "rafting_feedback")
    interval_secs = int(os.getenv("RAFTING_INTERVAL_SECONDS", 2))

    # Step 4: Verify the JSON data file exists
    if not DATA_FILE.exists():
        logger.error(f"❌ Data file not found: {DATA_FILE}. Exiting.")
        sys.exit(1)

    # Step 5: Create Kafka producer
    producer = create_kafka_producer(
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    if not producer:
        logger.error("❌ Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Step 6: Create Kafka topic if it doesn’t exist
    try:
        create_kafka_topic(topic)
        logger.info(f"✅ Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"❌ Failed to create topic '{topic}': {e}")
        sys.exit(1)

    # Step 7: Stream messages to Kafka
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as json_file:
            json_data = json.load(json_file)

            for message_dict in json_data:
                producer.send(topic, value=message_dict)
                logger.info(f"📨 Sent message to Kafka: {message_dict}")
                time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("⛔ Producer interrupted by user.")
    except Exception as e:
        logger.error(f"❌ Error during message production: {e}")
    finally:
        producer.close()
        logger.info("🔻 Kafka producer closed.")

    logger.info("✅ END: Rafting Producer")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
