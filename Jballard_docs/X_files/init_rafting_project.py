import os
import json
import pathlib
import subprocess
from dotenv import load_dotenv
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

# Load environment variables
load_dotenv()

# Define required environment variables
REQUIRED_ENV_VARS = [
    "KAFKA_BROKER_ADDRESS", "RAFTING_TOPIC",
    "RAFTING_RAW_TOPIC", "RAFTING_PROCESSED_TOPIC",
    "WEATHER_DATA_FILE", "RIVER_FLOW_DATA_FILE",
    "LOG_FILE"
]

# Paths
LOG_DIR = os.path.dirname(os.getenv("LOG_FILE", "logs/app.log"))
DATA_FILES = [
    os.getenv("WEATHER_DATA_FILE", "data/weather_data.json"),
    os.getenv("RIVER_FLOW_DATA_FILE", "data/river_flow.json")
]

KAFKA_BROKER = os.getenv("KAFKA_BROKER_ADDRESS", "localhost:9092")
RAFTING_TOPICS = [
    os.getenv("RAFTING_RAW_TOPIC", "rafting_feedback"),
    os.getenv("RAFTING_PROCESSED_TOPIC", "processed_csv_feedback")
]

def check_env_variables():
    """Ensure all required environment variables are set."""
    missing_vars = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]
    if missing_vars:
        print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        exit(1)
    print("✅ All environment variables are set.")

def check_kafka_connection():
    """Ensure Kafka is running and accessible."""
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
        topics = admin_client.list_topics()
        print(f"✅ Kafka connection successful. Existing topics: {topics}")
        return admin_client
    except Exception as e:
        print(f"❌ Kafka is not running or unreachable: {e}")
        exit(1)

def ensure_topics_exist(admin_client):
    """Ensure required Kafka topics exist."""
    existing_topics = admin_client.list_topics()
    new_topics = [
        NewTopic(name=topic, num_partitions=1, replication_factor=1)
        for topic in RAFTING_TOPICS if topic not in existing_topics
    ]

    if new_topics:
        admin_client.create_topics(new_topics=new_topics)
        print(f"✅ Created missing Kafka topics: {[t.name for t in new_topics]}")
    else:
        print("✅ All required Kafka topics already exist.")

def check_data_files():
    """Ensure required data files exist."""
    for file in DATA_FILES:
        if not pathlib.Path(file).exists():
            print(f"⚠️ Missing data file: {file}. Creating an empty file.")
            pathlib.Path(file).parent.mkdir(parents=True, exist_ok=True)
            with open(file, "w") as f:
                json.dump([], f)
    print("✅ All required data files are present.")

def prepare_logs():
    """Ensure log directory exists."""
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
        print(f"✅ Log directory created: {LOG_DIR}")
    else:
        print("✅ Log directory already exists.")

def start_kafka_services():
    """Prompt user to start Kafka services or restart if needed."""
    response = input("🔄 Would you like to start Kafka services? (yes/no): ").strip().lower()
    if response == "yes":
        subprocess.run(["zookeeper-server-start.sh", "-daemon", "/path/to/config/zookeeper.properties"])
        subprocess.run(["kafka-server-start.sh", "-daemon", "/path/to/config/server.properties"])
        print("✅ Kafka services started.")

def start_producers_consumers():
    """Prompt to start Kafka producers and consumers."""
    response = input("🚀 Start producers and consumers? (yes/no): ").strip().lower()
    if response == "yes":
        subprocess.run(["python", "rafting_producer.py"])
        subprocess.run(["python", "rafting_consumer.py"])
        subprocess.run(["python", "csv_rafting_consumer.py"])
        subprocess.run(["python", "csv_rafting_producer.py"])
        print("✅ Producers and Consumers started.")

if __name__ == "__main__":
    print("🔄 Initializing Rafting Feedback Streaming Project...")

    check_env_variables()
    admin_client = check_kafka_connection()
    ensure_topics_exist(admin_client)
    check_data_files()
    prepare_logs()
    start_kafka_services()
    start_producers_consumers()

    print("🎉 Initialization Complete!")
