import json
import time
from pathlib import Path
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

DATA_FILE = Path("data/weather.json")

# Prepare Matplotlib figure
fig, ax = plt.subplots()
x_data, y_data = [], []

def update(frame):
    """Update the chart with the latest weather data."""
    if DATA_FILE.exists():
        with open(DATA_FILE, "r") as f:
            data = json.load(f)
            x_data.append(time.strftime("%H:%M:%S", time.gmtime(data["timestamp"])))
            y_data.append(data["temperature"])
            x_data[:] = x_data[-10:]  # Keep only the last 10 data points
            y_data[:] = y_data[-10:]

        ax.clear()
        ax.plot(x_data, y_data, marker="o", linestyle="-")
        ax.set_title("Real-Time Temperature")
        ax.set_xlabel("Time")
        ax.set_ylabel("Temperature (°F)")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()

if __name__ == "__main__":
    ani = FuncAnimation(fig, update, interval=5000)  # Update every 5 seconds
    plt.show()
