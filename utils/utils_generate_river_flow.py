"""
utils_generate_river_flow.py

Generates river flow data for rafting trips.
"""
from datetime import datetime, timedelta
import random
import json
import pathlib

def generate_river_flow_data(output_file="data/river_flow.json"):
    """Generate river flow and water level data."""
    data_folder = pathlib.Path(output_file).parent
    data_folder.mkdir(exist_ok=True)  # Ensure the directory exists
    data_file = pathlib.Path(output_file)

    memorial_day_2024 = datetime(2024, 5, 27)
    labor_day_2024 = datetime(2024, 9, 2)
    date_range = (labor_day_2024 - memorial_day_2024).days

    river_data = [
        {
            "date": (memorial_day_2024 + timedelta(days=i)).strftime("%Y-%m-%d"),
            "river_flow": random.randint(800, 2000),  # Flow rate in cubic feet per second
            "water_level": round(random.uniform(2.5, 5.0), 2),  # Water level in feet
            "water_temperature": random.randint(55, 75)  # Temperature in Fahrenheit
        }
        for i in range(date_range + 1)
    ]

     # Save to a JSON file
    with open(data_file, "w") as file:
        json.dump(river_data, file, indent=4)

    return data_file  # Return the path for confirmation

# Example usage:
if __name__ == "__main__":
    generated_file = generate_river_flow_data()
    print(f"Generated river flow file: {generated_file}")