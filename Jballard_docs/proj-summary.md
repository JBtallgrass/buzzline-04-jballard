Summary of Components
    1. README.md (Project Intent & Goals)

        ○ Clearly outlines the problem statement, goals, and expected insights.
        ○ Emphasizes real-time streaming of rafting feedback and environmental data.
        ○ Provides setup instructions and links for prerequisites.
        ○ Introduces predictive insights and performance tracking.
    2. Rafting Producer (rafting_producer.py):
        ○ Generates and streams JSON feedback data into Kafka (rafting_feedback topic).
        ○ Runs multiple data generation scripts (utils_generate_rafting_data.py, utils_generate_river_flow.py, utils_generate_weather_data.py).
        ○ Handles errors gracefully and ensures Kafka services are running.
        ○ ✅ Good logging and structured steps for producer execution.
    3. CSV Rafting Producer (csv_rafting_producer.py):
        ○ Consumes CSV-style messages from rafting_csv_feedback and republishes to processed_csv_feedback.
        ○ Assigns trip status and flags disruptions based on bad weather conditions.
        ○ ✅ Structured message transformation and clear processing logic.
        ○ 🔧 Consider: More flexible topic configuration (allow env variables for topics).
    4. Rafting Consumer (rafting_consumer.py):
        ○ Reads rafting trip feedback and enriches it with weather & river conditions.
        ○ Flags negative comments and logs them separately.
        ○ Maintains weekly guide performance trends.
        ○ Saves negative feedback to JSON for analysis.
        ○ ✅ Great for monitoring trends and improving guide accountability.
        ○ 🔧 Consider: Configurable logging thresholds for alerts (high negativity weeks).
    5. CSV Rafting Consumer (csv_rafting_consumer.py):
        ○ Reads JSON feedback and transforms it into CSV-friendly format.
        ○ Republishes structured feedback to rafting_csv_feedback.
        ○ Logs weather & river conditions alongside feedback.
        ○ ✅ Great step for structured reporting and integration into analytical tools.
🔧 Consider: Support for additional metadata (e.g., timestamps for better trend analysis)