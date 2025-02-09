import os
import re
import csv
import json
from datetime import datetime

# Path to the log file
LOG_FILE_PATH = "logs/rafting_project_log.log"
CSV_OUTPUT_PATH = "data/rafting_feedback.csv"

# Define CSV columns
CSV_COLUMNS = ["timestamp", "date", "guide", "comment", "trip_type", "is_negative", 
               "weather", "temperature", "wind_speed", "rainfall", 
               "river_flow", "water_level", "water_temperature"]

def extract_json_from_log(log_line):
    """Extract JSON data from a log line using regex."""
    match = re.search(r"Sent message to Kafka: ({.*})", log_line)
    if match:
        return json.loads(match.group(1))
    return None

def extract_weather_from_log(log_line):
    """Extract weather-related data from log entries."""
    match = re.search(r"🌤 (.*?) \| 🌡 (\d+)°F \| 💨 Wind (\d+) mph \| 🌧 (.*?) inches rain", log_line)
    if match:
        return {
            "weather": match.group(1),
            "temperature": int(match.group(2)),
            "wind_speed": int(match.group(3)),
            "rainfall": float(match.group(4))
        }
    return None

def extract_river_data_from_log(log_line):
    """Extract river conditions from log entries."""
    match = re.search(r"Flow (\d+) cfs \| 📏 Water Level (.*?) ft \| 🌡 Water Temp (\d+)°F", log_line)
    if match:
        return {
            "river_flow": int(match.group(1)),
            "water_level": float(match.group(2)),
            "water_temperature": int(match.group(3))
        }
    return None

def convert_log_to_csv():
    """Read JSON messages from log, extract data, and write to CSV."""
    data_records = []
    current_weather = {}
    current_river_data = {}

    with open(LOG_FILE_PATH, "r", encoding="utf-8") as log_file:
        for line in log_file:
            json_data = extract_json_from_log(line)
            if json_data:
                record = {
                    "timestamp": json_data.get("timestamp"),
                    "date": json_data.get("date"),
                    "guide": json_data.get("guide"),
                    "comment": json_data.get("comment"),
                    "trip_type": json_data.get("trip_type"),
                    "is_negative": json_data.get("is_negative"),
                    **current_weather,  # Add latest weather data
                    **current_river_data  # Add latest river data
                }
                data_records.append(record)
            
            weather_data = extract_weather_from_log(line)
            if weather_data:
                current_weather = weather_data
            
            river_data = extract_river_data_from_log(line)
            if river_data:
                current_river_data = river_data

    # Write extracted data to CSV
    with open(CSV_OUTPUT_PATH, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(data_records)

    print(f"✅ Converted log file to CSV: {CSV_OUTPUT_PATH}")

if __name__ == "__main__":
    convert_log_to_csv()
