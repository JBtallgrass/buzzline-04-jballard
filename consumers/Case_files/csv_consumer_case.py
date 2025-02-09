"""
csv_consumer_case.py

Consume json messages from a Kafka topic and visualize author counts in real-time.

Example Kafka message format:
{"timestamp": "2025-01-11T18:15:00Z", "temperature": 225.0}

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing

# Use a deque ("deck") - a double-ended queue data structure
# A deque is a good way to monitor a certain number of "most recent" messages
# A deque is a great data structure for time windows (e.g. the last 5 messages)
from collections import deque

# Import external packages
from dotenv import load_dotenv

# IMPORTANT
# Import Matplotlib.pyplot for live plotting
# Use the common alias 'plt' for Matplotlib.pyplot
# Know pyplot well
import matplotlib.pyplot as plt

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("SMOKER_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("SMOKER_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


def get_stall_threshold() -> float:
    """Fetch message interval from environment or use default."""
    temp_variation = float(os.getenv("SMOKER_STALL_THRESHOLD_F", 0.2))
    return temp_variation


def get_rolling_window_size() -> int:
    """Fetch rolling window size from environment or use default."""
    window_size = int(os.getenv("SMOKER_ROLLING_WINDOW_SIZE", 5))
    logger.info(f"Rolling window size: {window_size}")
    return window_size


#####################################
# Set up data structures (empty lists)
#####################################

timestamps = []  # To store timestamps for the x-axis
temperatures = []  # To store temperature readings for the y-axis

#####################################
# Set up live visuals
#####################################

# Use the subplots() method to create a tuple containing
# two objects at once:
# - a figure (which can have many axis)
# - an axis (what they call a chart in Matplotlib)
fig, ax = plt.subplots()

# Use the ion() method (stands for "interactive on")
# to turn on interactive mode for live updates
plt.ion()


#####################################
# Define a function to detect a stall
#####################################


def detect_stall(rolling_window_deque: deque, window_size: int) -> bool:
    """
    Detect a temperature stall based on the rolling window.

    Args:
        rolling_window_deque (deque): Rolling window of temperature readings.

    Returns:
        bool: True if a stall is detected, False otherwise.
    """
    if len(rolling_window_deque) < window_size:
        # We don't have a full deque yet
        # Keep reading until the deque is full
        logger.debug(
            f"Rolling window current size: {len(rolling_window_deque)}. Waiting for {window_size}."
        )
        return False

    # Once the deque is full we can calculate the temperature range
    # Use Python's built-in min() and max() functions
    # If the range is less than or equal to the threshold, we have a stall
    # And our food is ready :)
    temp_range = max(rolling_window_deque) - min(rolling_window_deque)
    is_stalled: bool = temp_range <= get_stall_threshold()
    if is_stalled:
        logger.debug(f"Temperature range: {temp_range}°F. Stalled: {is_stalled}")
    return is_stalled


#####################################
# Define an update chart function for live plotting
# This will get called every time a new message is processed
#####################################


def update_chart(rolling_window, window_size):
    """
    Update temperature vs. time chart.
    Args:
        rolling_window (deque): Rolling window of temperature readings.
        window_size (int): Size of the rolling window.
    """
    # Clear the previous chart
    ax.clear()  

    # Create a line chart using the plot() method
    # Use the timestamps for the x-axis and temperatures for the y-axis
    # Use the label parameter to add a legend entry
    # Use the color parameter to set the line color
    ax.plot(timestamps, temperatures, label="Temperature", color="blue")

    # Use the built-in axes methods to set the labels and title
    ax.set_xlabel("Time")
    ax.set_ylabel("Temperature (°F)")
    ax.set_title("Smart Smoker: Temperature vs. Time")

    # Highlight stall points if conditions are met such that
    #    The rolling window is full and a stall is detected
    if len(rolling_window) >= window_size and detect_stall(rolling_window, window_size):
        # Mark the stall point on the chart

        # An index of -1 gets the last element in a list
        stall_time = timestamps[-1]
        stall_temp = temperatures[-1]

        # Use the scatter() method to plot a point
        # Pass in the x value as a list, the y value as a list (using [])
        # and set the marker color and label
        # zorder is used to ensure the point is plotted on TOP of the line chart
        # zorder of 5 is higher than the default zorder of 2
        ax.scatter(
            [stall_time], [stall_temp], color="red", label="Stall Detected", zorder=5
        )

        # Use the annotate() method to add a text label
        # To learn more, look up the matplotlib axes.annotate documentation
        # https://matplotlib.org/stable/api/_as_gen/matplotlib.axes.Axes.annotate.html
        # textcoords="offset points" means the label is placed relative to the point
        # xytext=(10, -10) means the label is placed 10 points to the right and 10 points down from the point 
        # Typically, the first number is x (horizontal) and the second is y (vertical)
        # x: Positive moves to the right, negative to the left
        # y: Positive moves up, negative moves down
        # ha stands for horizontal alignment
        # We set color to red, a common convention for warnings
        ax.annotate(
            "Stall Detected",
            (stall_time, stall_temp),
            textcoords="offset points",
            xytext=(10, -10),
            ha="center",
            color="red",
        )

    # Regardless of whether a stall is detected, we want to show the legend

    # Use the legend() method to display the legend
    ax.legend()

    # Use the autofmt_xdate() method to automatically format the x-axis labels as dates
    fig.autofmt_xdate()

    # Use the tight_layout() method to automatically adjust the padding
    plt.tight_layout()

    # Draw the chart
    plt.draw()

    # Pause briefly to allow some time for the chart to render
    plt.pause(0.01)  


#####################################
# Function to process a single message
# #####################################


def process_message(message: str, rolling_window: deque, window_size: int) -> None:
    """
    Process a JSON-transferred CSV message and check for stalls.

    Args:
        message (str): JSON message received from Kafka.
        rolling_window (deque): Rolling window of temperature readings.
        window_size (int): Size of the rolling window.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        data: dict = json.loads(message)
        temperature = data.get("temperature")
        timestamp = data.get("timestamp")
        logger.info(f"Processed JSON message: {data}")

        # Ensure the required fields are present
        if temperature is None or timestamp is None:
            logger.error(f"Invalid message format: {message}")
            return

        # Append the temperature reading to the rolling window
        rolling_window.append(temperature)

        # Append the timestamp and temperature to the chart data
        timestamps.append(timestamp)
        temperatures.append(temperature)

        # Update chart after processing this message
        update_chart(rolling_window=rolling_window, window_size=window_size)

        # Check for a stall
        if detect_stall(rolling_window, window_size):
            logger.info(
                f"STALL DETECTED at {timestamp}: Temp stable at {temperature}°F over last {window_size} readings."
            )

    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Polls messages and updates a live chart.
    """
    logger.info("START consumer.")

    # Clear previous run's data
    timestamps.clear()
    temperatures.clear()

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")
    logger.info(f"Rolling window size: {window_size}")
    rolling_window = deque(maxlen=window_size)

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str, rolling_window, window_size)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")


#####################################
# Conditional Execution
#####################################

# Ensures this script runs only when executed directly (not when imported as a module).
if __name__ == "__main__":
    main()
    plt.ioff()  # Turn off interactive mode after completion
    plt.show()
