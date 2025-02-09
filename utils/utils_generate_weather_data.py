"""
utils_generate_weather_data.py

Generates synthetic weather data for each rafting trip day.
"""
from datetime import datetime, timedelta
import random
import json
import pathlib

# Define weather conditions
WEATHER_CONDITIONS = ["Sunny", "Cloudy", "Rainy", "Stormy"]

def generate_weather_data(output_file="data/weather_conditions.json"):
    """Generate weather data for rafting trip dates."""
    data_folder = pathlib.Path(output_file).parent
    data_folder.mkdir(exist_ok=True)  # Ensure the directory exists
    data_file = pathlib.Path(output_file)

    memorial_day_2024 = datetime(2024, 5, 27)
    labor_day_2024 = datetime(2024, 9, 2)
    date_range = (labor_day_2024 - memorial_day_2024).days

    weather_data = [
        {
            "date": (memorial_day_2024 + timedelta(days=i)).strftime("%Y-%m-%d"),
            "temperature": random.randint(60, 90),  # Random temp between 60°F - 90°F
            "weather_condition": random.choice(WEATHER_CONDITIONS),
            "wind_speed": random.randint(0, 20),  # Wind in mph
            "precipitation": round(random.uniform(0, 1), 2)  # Inches of rain
        }
        for i in range(date_range + 1)
    ]

    # Save to JSON file
    with open(data_file, "w") as f:
        json.dump(weather_data, f, indent=4)

    print(f"Weather data saved to {data_file}")
    return data_file

# Example usage:
if __name__ == "__main__":
    generated_file = generate_weather_data()
    print(f"Generated weather file: {generated_file}")