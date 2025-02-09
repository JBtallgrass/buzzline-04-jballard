### **Step 1: Prepare Your Environment**
1. **Ensure Kafka and Zookeeper are running**:  
   If you’re using a local Kafka setup, you’ll need to start both:
   ```bash
   bin/zookeeper-server-start.sh config/zookeeper.properties
   bin/kafka-server-start.sh config/server.properties
   ```
   > **Note:** You can use Docker if you prefer running Kafka in containers.

2. **Verify `.env` File**  
   Ensure your `.env` file is correctly configured with Kafka broker details and file paths:
   ```bash
   ZOOKEEPER_ADDRESS=localhost:2181
   KAFKA_BROKER_ADDRESS=localhost:9092
   RAFTING_TOPIC=rafting_feedback
   RAFTING_CSV_TOPIC=rafting_csv_feedback
   RAFTING_PROCESSED_TOPIC=processed_csv_feedback
   ```

---

### **Step 2: Run the Data Generation Scripts**
Before starting the Kafka consumers, **generate initial data** for weather, river flow, and rafting feedback.

1. **Run the data generator scripts manually or through `rafting_producer.py`:**
   ```bash
   python utils_generate_weather_data.py
   python utils_generate_river_flow.py
   python utils_generate_rafting_data.py
   ```
   OR use `rafting_producer.py`, which runs all these scripts in sequence:
   ```bash
   python rafting_producer.py
   ```

---

### **Step 3: Start the Producers**
- **`rafting_producer.py`**: This script reads from the generated data (`all_rafting_remarks.json`) and publishes messages to the Kafka topic (`rafting_feedback`).  
   Run it like this:
   ```bash
   python rafting_producer.py
   ```

---

### **Step 4: Start the Consumers**
You have **multiple consumers**, each with a specific purpose. Here’s how you should sequence them:

1. **`rafting_consumer.py`**  
   - **Consumes** raw rafting feedback from `rafting_feedback`.
   - **Logs** feedback, weather, and river conditions.
   - **Tracks weekly guide performance**.

   ```bash
   python rafting_consumer.py
   ```

2. **`csv_rafting_consumer.py`**  
   - **Processes feedback** and republishes structured data to `rafting_csv_feedback`.  
   - **Logs weather and river conditions**.

   ```bash
   python csv_rafting_consumer.py
   ```

3. **`csv_feedback_consumer.py`**  
   - **Consumes structured feedback** from `rafting_csv_feedback`.
   - **Saves processed feedback to a CSV file (`data/rafting_feedback.csv`)**.

   ```bash
   python csv_feedback_consumer.py
   ```

4. **`jb_project_consumer.py`**  
   - **Real-time visualization** of feedback and environmental data.  
   - Generates and saves charts for:
     - Positive vs. Negative feedback
     - Weekly feedback trends
     - Weather impact on negative feedback
     - River flow box plots.

   ```bash
   python jb_project_consumer.py
   ```

---

### **Summary of the Flow:**

1. **Data Generation** → Run `rafting_producer.py` to generate and publish data.  
2. **Kafka Consumers** → Start each consumer in sequence:
   - `rafting_consumer.py`: Logs and analyzes raw feedback.
   - `csv_rafting_consumer.py`: Converts feedback to structured CSV format.
   - `csv_feedback_consumer.py`: Saves processed feedback to a CSV file.
   - `jb_project_consumer.py`: Creates real-time visualizations.

---

### **Pro Tips for Execution:**
- **Run each consumer in a separate terminal tab** so you can monitor logs in real-time.  
- **Monitor Kafka topics**: Use the Kafka command-line tool to check if messages are being published and consumed:
   ```bash
   kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic rafting_feedback --from-beginning
   ```

Would you like me to provide a **script to automate the sequence**, or do you prefer running each manually?