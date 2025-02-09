"""
Logger Setup Script
File: utils/utils_logger.py

This script provides logging functions for the rafting project.
Logs all rafting feedback (positive & negative) and flags negative comments with 🛑.
"""

# Imports from Python Standard Library
import pathlib

# Imports from external packages
from loguru import logger

# Get this file name without the extension
CURRENT_SCRIPT = pathlib.Path(__file__).stem

# Set directory where logs will be stored
LOG_FOLDER: pathlib.Path = pathlib.Path("logs")

# Set the name of the rafting log file
LOG_FILE: pathlib.Path = LOG_FOLDER.joinpath("rafting_project_log.log")

# Ensure the log folder exists or create it
try:
    LOG_FOLDER.mkdir(exist_ok=True)
    logger.info(f"Log folder created at: {LOG_FOLDER}")
except Exception as e:
    logger.error(f"Error creating log folder: {e}")

# Configure Loguru to write to the rafting log file
try:
    logger.add(LOG_FILE, level="INFO", format="{time} | {level} | {message}")
    logger.info(f"Logging rafting feedback to file: {LOG_FILE}")
except Exception as e:
    logger.error(f"Error configuring logger to write to file: {e}")


def log_feedback(guide: str, comment: str, is_negative: bool, trip_date: str, weather_summary: str, river_summary: str) -> None:
    """
    Logs rafting feedback.

    Args:
        guide (str): The name of the rafting guide.
        comment (str): The customer's feedback comment.
        is_negative (bool): Whether the feedback is negative.
        trip_date (str): The date of the rafting trip.
        weather_summary (str): Weather conditions on the trip date.
        river_summary (str): River flow conditions on the trip date.
    """
    try:
        # Flag negative comments with 🛑
        comment_display = f"🛑 {comment}" if is_negative else comment

        # Log structured feedback
        logger.info(f"📝 Feedback ({trip_date}) | Guide: {guide} | Comment: {comment_display}")
        logger.info(f"⛅ {weather_summary}")
        logger.info(f"🌊 {river_summary}")

        if is_negative:
            logger.warning(f"🛑 NEGATIVE FEEDBACK for {guide} on {trip_date}: {comment}")

    except Exception as e:
        logger.error(f"Error logging rafting feedback: {e}")


def get_log_file_path() -> pathlib.Path:
    """Return the path to the rafting log file."""
    return LOG_FILE


def main() -> None:
    """Main function to execute logger setup and demonstrate its usage."""
    logger.info(f"STARTING {CURRENT_SCRIPT}.py")

    # Example feedback log
    log_feedback(
        guide="Samantha",
        comment="Amazing trip! Our guide was fantastic!",
        is_negative=False,
        trip_date="2024-07-04",
        weather_summary="Sunny | 85°F | Wind 10 mph | No Rain",
        river_summary="Flow 1200 cfs | Water Level 3.5 ft | Water Temp 68°F"
    )

    log_feedback(
        guide="Emily",
        comment="🛑 The guide was not engaging. Not what I expected.",
        is_negative=True,
        trip_date="2024-07-04",
        weather_summary="Sunny | 85°F | Wind 10 mph | No Rain",
        river_summary="Flow 1200 cfs | Water Level 3.5 ft | Water Temp 68°F"
    )

    logger.info(f"View the log output at {LOG_FILE}")
    logger.info(f"EXITING {CURRENT_SCRIPT}.py.")


# Conditional execution block that calls main() only when this file is executed directly
if __name__ == "__main__":
    main()
