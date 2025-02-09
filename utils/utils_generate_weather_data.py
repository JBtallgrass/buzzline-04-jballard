from datetime import datetime, timedelta
import random
import json
import pathlib
import math

# Define realistic weather conditions for the region
WEATHER_CONDITIONS = ["Sunny", "Partly Cloudy", "Cloudy", "Rainy", "Stormy"]

def calculate_feels_like(temperature, wind_speed):
    """Calculate 'feels like' temperature using wind chill for cold temps and heat index for warm temps."""
    if temperature >= 80:
        # Simple heat index approximation for warm temperatures
        feels_like = temperature + 0.05 * temperature
    elif temperature < 50 and wind_speed > 3:
        # Wind chill calculation
        feels_like = 35.74 + 0.6215 * temperature - 35.75 * math.pow(wind_speed, 0.16) + 0.4275 * temperature * math.pow(wind_speed, 0.16)
    else:
        feels_like = temperature
    return round(feels_like, 1)

def generate_weather_data(output_file="data/weather_conditions.json"):
    """Generate realistic synthetic weather data for the French Broad River region from May to September 2024."""
    data_folder = pathlib.Path(output_file).parent
    data_folder.mkdir(parents=True, exist_ok=True)
    data_file = pathlib.Path(output_file)

    memorial_day_2024 = datetime(2024, 5, 24)
    labor_day_2024 = datetime(2024, 9, 2)
    date_range = (labor_day_2024 - memorial_day_2024).days

    weather_data = []

    for i in range(date_range + 1):
        current_date = memorial_day_2024 + timedelta(days=i)
        month = current_date.month

        # Adjust temperature range based on the month
        if month in [5, 9]:  # May and September are cooler
            temperature = random.randint(55, 85)
        else:  # June, July, August are warmer
            temperature = random.randint(65, 95)
        
        # Set precipitation likelihood based on weather condition
        weather_condition = random.choice(WEATHER_CONDITIONS)
        precipitation = round(random.uniform(0.1, 2.0), 2) if weather_condition in ["Rainy", "Stormy"] else 0

        # Generate the weather record
        weather_record = {
            "date": current_date.strftime("%Y-%m-%d"),
            "temperature": temperature,
            "weather_condition": weather_condition,
            "humidity": random.randint(30, 90),  # Realistic humidity range for the area
            "wind_speed": random.randint(0, 20),  # Wind speed in mph
            "precipitation": precipitation,  # Inches of rain
            "uv_index": random.randint(1, 11),  # UV index from 1 (low) to 11+ (extreme)
            "feels_like": calculate_feels_like(temperature, random.randint(0, 20))
        }

        weather_data.append(weather_record)

    # Save to JSON file
    with open(data_file, "w") as f:
        json.dump(weather_data, f, indent=4)

    print(f"Weather data saved to {data_file} with {len(weather_data)} records.")
    return data_file

# Example usage:
if __name__ == "__main__":
    generated_file = generate_weather_data()
    print(f"Generated weather file: {generated_file}")
