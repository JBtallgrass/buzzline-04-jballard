import json
import time
from pathlib import Path

DATA_FILE = Path("data/weather.json")
DATA_FILE.parent.mkdir(parents=True, exist_ok=True)  # Ensure data directory exists

def generate_weather_data():
    """Generate weather data and write to a JSON file."""
    weather_conditions = ["Sunny", "Cloudy", "Rainy", "Stormy"]
    while True:
        weather_data = {
            "temperature": 60 + int(time.time()) % 20,  # Simulated temperature
            "weather_condition": weather_conditions[int(time.time()) % len(weather_conditions)],
            "timestamp": time.time()
        }
        with open(DATA_FILE, "w") as f:
            json.dump(weather_data, f, indent=4)
        print(f"📤 Generated weather data: {weather_data}")
        time.sleep(2)  # Update every 2 seconds

if __name__ == "__main__":
    generate_weather_data()
